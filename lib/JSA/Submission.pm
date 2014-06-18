package JSA::Submission;

=head1 NAME

JSA::Submission - Common routines for DP job submission scripts.

=cut

use File::Basename;
use File::Spec;
use Net::SMTP;

use OMP::ArcQuery;
use OMP::ArchiveDB;
use JSA::Headers::CADC qw/correct_asn_id/;

use warnings;
use strict;

use parent qw/Exporter/;
our @EXPORT_OK = qw/%DR_RECIPES %BAD_OBSIDSS %JUNK_OBSIDSS
                    all_messages assign_to_group echo_messages
                    get_obsidss log_message obs_is_fts2_or_pol2_RECIPE
                    prepare_archive_db send_log_email write_log_file/;

=head1 DATA

=over 4

=item %DR_RECIPES

SLEDGE HAMMER HACK

We need to be able to control pipeline recipe names when doing night
processing. Currently the submission script will supply an project
based recipe parameter file in project mode but not in night mode. This
will not trigger a completely different recipe though. For now we
have a local hash rather than an external config file during submission.
We provide a recipe override based on the most recent project id added
to the group. We only need to override blank field observations for SCUBA-2.

=cut

our %DR_RECIPES = (
                  M09BGT01 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI152 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI155 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI143 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI115 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI128 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI136 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI120 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI101 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI109 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI130 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI104 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI149 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI134 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI145 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI114 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BI142 => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BH101A => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BH102A => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BH103A => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BH105A => "REDUCE_SCAN_FAINT_POINT_SOURCES",
                  M09BH106B => "REDUCE_SCAN_FAINT_POINT_SOURCES",
);

=item %BAD_OBSIDSS

HACK 2: The OMP can not yet handle cases where one subsystem has
a good data set and the other has a bad data set. Here we list
OBSIDSS values for bad subsystems where the other half is good.
They will not be included in group processing.

=cut

our %BAD_OBSIDSS = map { $_ => undef } qw/
                     scuba2_28_20100223T051545_450
                     scuba2_29_20100223T052321_450
                     scuba2_67_20100306T152738_450
                     /;

=item %JUNK_OBSIDSS

These subsystems should never be processed. Not even in night mode.

=cut

our %JUNK_OBSIDSS = map { $_ => undef } qw/
                     scuba2_34_20100111T083310_450
                     scuba2_6_20091203T050120_450
                     scuba2_28_20091205T064832_450
                     scuba2_23_20100111T063043_450
                     scuba2_49_20100111T111043_450
                     scuba2_18_20100112T043655_450
                     scuba2_29_20100223T052321_450
                     scuba2_78_20100225T120802_850
                     scuba2_105_20100310T151153_450
                     scuba2_106_20100310T152601_450
                     scuba2_68_20100306T153932_450
                     scuba2_66_20100306T151319_450
                     scuba2_9_20100304T040112_450
                     /;

=back

=head1 SUBROUTINES

=over 4

=item prepare_archive_db

Configure the archive database for querying.

=cut

sub prepare_archive_db {
  # Use new JCMT database for DAS data in ACSIS format.
  $OMP::ArcQuery::GSD_FROM_JCMT_INSTEAD = 1;

  # Don't fall back to files.
  $OMP::ArchiveDB::FallbackToFiles = 0;

  # Use DB for any date.
  $OMP::ArchiveDB::AnyDate = 1;

  # Fix search criteria to avoid being reset just before querying for data.
  OMP::ArchiveDB->use_existing_criteria( 1 );
}

=item get_obsidss

Quick routine to retrieve OBSIDSS
Multiple headers can be supplied

=cut

sub get_obsidss {
  my $obsid = shift;
  my @hdrs = @_;

  # Try not to merge headers if the answer is in the small one
  my $obsidss;
 KEYS: for my $k ( qw/ OBSID_SUBSYSNR OBSIDSS SUBSYSNR / ) {
    # try each key in turn
    for my $hdr (@hdrs) {
      if (exists $hdr->{$k}) {
        if ($k eq 'SUBSYSNR') {
          $obsidss = $obsid . "_" . $hdr->{$k};

          # HACK: SCUBA-2 has a buggy OBSIDSS in that
          # the zero-padding is different
          $obsidss =~ s/^scuba2_0*/scuba2_/;

        } else {
          $obsidss = $hdr->{$k};
        }
        last KEYS;
      }
    }
  }
  if (!defined $obsidss) {
    die "Could not work out OBSIDSS for ". $obsid;
  }
  return $obsidss;
}

=item assign_to_group

Assign the observation to a particular group

=cut

sub assign_to_group {
  my $instrument = shift;
  my $obsid = shift;
  my $frameclass = shift;
  my $not_in_group = shift;
  my $hdrref = shift;
  my $curref = shift;
  my $fileref = shift;
  my $groups = shift;

  # Deref some hashes and arrays
  my %current = %$curref;
  my %tmphdr = %$hdrref;

  # Strip any paths
  my @files = map { basename($_) } @$fileref;

  # Set ORAC_INSTRUMENT so SCUBA-2 works.
  my $ORAC_INSTRUMENT = '';
  if( exists $tmphdr{SUBSYSNR} &&
      defined $tmphdr{SUBSYSNR}  &&
      $instrument eq 'SCUBA-2' ) {
    $ORAC_INSTRUMENT = 'SCUBA2_' . $tmphdr{SUBSYSNR};
  }
  $ENV{'ORAC_INSTRUMENT'} = $ORAC_INSTRUMENT;

  # if not_in_group is false then we have to determine the
  # grouping scheme. If it is false then we need to use the OBSIDSS
  my $group;
  if ($not_in_group) {
    $group = get_obsidss( $obsid, \%tmphdr );
  } else {
    my $frm = new $frameclass;
    $frm->hdr( %tmphdr );
    $frm->findgroup;
    $group = $frm->asn_id;
  }

  # Now correct for the association identifier
  $tmphdr{ASN_ID} = $group;
  $group = correct_asn_id( $current{mode}, \%tmphdr );

  push @{$groups->{$group}{files}}, @files;
  $groups->{$group}{mode} = $current{mode};
  $groups->{$group}{drparams} = $current{drparams} if defined $current{drparams};
  $groups->{$group}{recpars} = $current{recpars} if defined $current{recpars};
  # Only set if either we have no previous value for dprecipe or if the
  # previous value is lower than the current value (so this observation
  # needs more resources than a previous group member)
  if (defined $current{dprecipe}) {
    if (!exists $groups->{$group}{dprecipe} ||
        (exists $groups->{$group}{dprecipe} && $groups->{$group}{dprecipe} < $current{dprecipe})) {
      $groups->{$group}{dprecipe} = $current{dprecipe};
    }
  }
  return $group;
}

=item obs_is_fts2_or_pol2_RECIPE

Tell if an observation is FTS-2 or POL-2 type by recipe.

=cut

{
  my %skip_recipe;

  sub obs_is_fts2_or_pol2_RECIPE {

    my ( $backend, $recipe ) = @_;

    defined $recipe && defined $backend && $backend =~ m{^ scuba-?2 $}xi
      or return;

    unless ( keys %skip_recipe ) {

      %skip_recipe = map { $_ => undef } qw(  REDUCE_FTS2
                                              REDUCE_FTS_FOCUS
                                              REDUCE_FTS_POINTING
                                              REDUCE_FTS_SCAN
                                              REDUCE_FTS_ZPD
                                              REDUCE_POL_STARE
                                              REDUCE_DREAMSTARE
                                            );
    }

    return exists $skip_recipe{ uc $recipe };
  }
}

=item echo_messages($echo)

Enable or disable immediate printing of log messages.

=item log_message

Always cache. Sometimes print.

=item all_messages

Return all cached messages.

=cut

{
  # Cache for messages, or we just print them straight out
  my @MESSAGES;
  my $ECHO = 0;

  sub echo_messages {
    $ECHO = shift;
  }

  sub log_message {
    my @msg = @_;
    if ($ECHO) {
      print @msg;
    }
    push(@MESSAGES, @msg);
  }

  sub all_messages {
    return @MESSAGES;
  }
}

=item write_log_file

Write to the logging directory.

=cut

sub write_log_file {
  my $title = shift;
  my $ut = shift;
  my $project = shift;

  my $logdir = "/jac_logs/jsa";
  my $froot = $title . "-" . (defined $ut ? $ut : $project) . ".log";
  my $outfile = File::Spec->catfile( $logdir, $froot );
  if (-d $logdir) {
    if (open( my $logfh, ">", $outfile ) ) {
      for my $msg (all_messages()) {
	print $logfh $msg;
      }
      close $logfh;
    }
  }
}

=item send_log_email

Send the email.

=cut

sub send_log_email {
  my $title = shift;
  my $ut = shift;
  my $project = shift;

  my $MAILHOST = 'mailhost.jach.hawaii.edu';
  my $MAILTO = 'jcmtarch@jach.hawaii.edu';
  my $MAILFROM = 'jcmtarch@jach.hawaii.edu';

  my $smtp = Net::SMTP->new( $MAILHOST );
  $smtp->mail( $MAILFROM );
  $smtp->to( $MAILTO );

  $smtp->data();
  $smtp->datasend( "To: $MAILTO\n" );
  $smtp->datasend("Subject: " . $title . " for " . ( defined( $ut ) ? $ut : $project ) . "\n");
  $smtp->datasend("\n");
  $smtp->datasend( all_messages() );

  $smtp->quit;
}

=back

=head1 COPYRIGHT

Copyright (C) 2009-2014 Science and Technology Facilities Council.
All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful,but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place,Suite 330, Boston, MA  02111-1307, USA

=cut

1;

# vim: sw=2 sts=2
