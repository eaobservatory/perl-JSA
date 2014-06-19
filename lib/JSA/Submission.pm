package JSA::Submission;

=head1 NAME

JSA::Submission - Common routines for DP job submission scripts.

=cut

use File::Basename;
use File::Spec;
use Net::SMTP;

use OMP::ArcQuery;
use OMP::ArchiveDB;
use OMP::Info::ObsGroup;

use JSA::CADC_DP qw/connect_to_cadcdp disconnect_from_cadcdp
                    create_recipe_instance dprecinst_url/;
use JSA::Error qw/:try/;
use JSA::Files qw/file_to_uri/;
use JSA::Headers::CADC qw/correct_asn_id/;

use warnings;
use strict;

use parent qw/Exporter/;
our @EXPORT_OK = qw/%DR_RECIPES %BAD_OBSIDSS %JUNK_OBSIDSS
                    all_messages assign_to_group
                    determine_resource_requirement echo_messages
                    find_observations
                    get_obsidss log_message obs_is_fts2_or_pol2_RECIPE
                    prepare_archive_db send_log_email submit_jobs
                    write_log_file/;

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

=item find_observations($ut, $project, $priority)

Determines the mode of operation and retieves the observation group.

  ($mode, $grp) = find_observations($ut, $project);

The project should already be in upper case.

=cut

sub find_observations {
  my $ut = shift;
  my $project = shift;
  my $priority = shift;

  my ($mode, $grp);

  # Make sure the UT parameter is defined.
  if( ! defined $ut && ! defined $project ) {
    die "Must include either -ut or -project parameter";
  } elsif (defined $ut && defined $project ) {
    die "Can not include both -ut and -project parameters";
  }

  my $pristring = 'with default priority';
  if (defined $priority) {
    $pristring = "with priority $priority";
  }

  if( defined( $project ) ) {
    $mode = "project";
    log_message( "Running jsasubmit for project $project $pristring.\n");
  } else {
    log_message( "Running jsasubmit for UT date $ut $pristring.\n");
    $mode = "night";
  }

  if( $mode eq 'project' ) {

    # Verify the project ID
    if (!OMP::ProjServer->verifyProject( $project )) {
      die "Project '$project' does not seem to exist in the database.\n";
    }

    $grp = new OMP::Info::ObsGroup( projectid => $project,
                                    nocomments => 0,
                                    retainhdr => 1 );
  } else {
    $grp = new OMP::Info::ObsGroup( telescope => 'JCMT',
                                    date => $ut,
                                    nocomments => 0,
                                    retainhdr => 1 );
  }

  return ($mode, $grp);
}

=item determine_resource_requirement($obs)

Make an estimate of the resources required to process the observation
The rule is something like:

=over 4

=item ACSIS

=over 4

=item HARP scan maps: 16G
=item Everything else: 8G

=back

=item SCUBA-2

=over 4

=item Observations longer than about 20 minutes: 16G
=item Everything else: 8G

=back

When we have multiple subarrays the 20 minutes will scale accordingly.
and we'll need to switch to a 64G queue for those.

=back

=cut

sub determine_resource_requirement {
  my $obs = shift;

  my $hdr = $obs->hdrhash();
  my $instrume = uc($hdr->{'INSTRUME'});

  my $req = JSA::CADC_DP::CADC_DPREC_8G;

  if ( $instrume eq 'SCUBA-2' ) {
    my $duration = $obs->endobs - $obs->startobs;
    # We count the number of files as a surrogate for required
    # computing resources since we know each file is roughly
    # same length. Assume 2 subsytems.
    my @files = $obs->filename;
    my $nfiles = @files / 2;
    # As of 20100929
    # 16 files => 6.2 GB
    # 22 files => 8.8GB
    # 27 files => 10.7GB
    # 62 files => 26.2 GB
    if ($nfiles > 30) {
      $req = JSA::CADC_DP::CADC_DPREC_64G;
    } elsif ($nfiles > 16) {
      $req = JSA::CADC_DP::CADC_DPREC_16G;
    }
  } elsif ( $instrume eq 'HARP' && $hdr->{SAM_MODE} =~ /scan|raster/i) {
    $req = JSA::CADC_DP::CADC_DPREC_16G;
  }

  return $req;
}

=item submit_jobs(\%groups, $atCADC, $mode, $priority, $queue, $debug)

Submit processing jobs to CADC's DP system.

C<\%groups> is a reference to a hash holding groups and information about them.
Keys are the group association id string.
Values are hash references containing keys:

=over 4

=item files

Array of files for this group.

=item drparams

DR parameters.

=item mode

Mode to use for processing.

=item dprecipe

DP recipe to use for this group.

=back

=cut

sub submit_jobs {
  my $_groups = shift;
  my $atCADC = shift;
  my $mode = shift;
  my $priority = shift;
  my $queue = shift;
  my $debug = shift;

  my %groups = %$_groups;

  my $dbh;
  if ($debug) {
    log_message( "Would be connecting to CADC data processing database here\n" );
  } else {
    log_message( "Connecting to CADC data processing database...");
    $dbh = connect_to_cadcdp;
    log_message( "connected!\n\n");
  }

  foreach my $group ( sort keys %groups ) {
    log_message( "Requesting CADC processing of the following files:\n");
    log_message( map { "$_\n" } @{$groups{$group}{files}});
    my @members = map { file_to_uri( $_ ) } @{$groups{$group}{files}};

    # Only check for night mode, in which case $atCADC will be defined.
    # Otherwise assume the files are at CADC.
    if( defined $atCADC ) {
      log_message( "Checking to ensure files are at CADC...\n");
      my $there = 1;
      foreach my $file ( @{$groups{$group}{files}} ) {
        $file =~ s/\.sdf$//;
        if( ! exists( $atCADC->{$file} ) ) {
          $there = 0;
          log_message( "$file is not at CADC!\n");
        }
      }
      if( ! $there ) {
        log_message( "One or more files from current group not at CADC.\nSkipping to next group.\n");
        next;
      }
      log_message( "All files are at CADC. Submitting processing request.\n");
    }

    # Submit the job.
    my $recipe_id;
    try {
      my %opts;
      $opts{'mode'} = ( exists $groups{$group}{mode} ? $groups{$group}{mode} : $mode);
      $opts{priority} = $priority if defined $priority;
      $opts{queue} = $queue if defined $queue;
      if ( exists $groups{$group}{drparams} ) {
        $opts{drparams} = $groups{$group}{drparams};
      }

      if (exists $groups{$group}{dprecipe}) {
        $opts{dprecipe} = $groups{$group}{dprecipe};
      }

      # recpars could be part of drparams but for now we let the CADC_DP code handle it
      if (exists $groups{$group}{recpars}) {
        $opts{recpars} = $groups{$group}{recpars};
      }

      $opts{tag} = $group;

      if ($debug) {
        log_message( "Would be submitting job with options:\n");
        for my $k (sort keys %opts) {
           log_message( "\t$k : ".(defined $opts{$k} ? $opts{$k} : "<undef>")."\n");
        }
        $recipe_id = "0xFabCADC";
      } else {
        $recipe_id = create_recipe_instance( $dbh, \@members, \%opts );
      }
    }
    catch JSA::Error::CADCDB with {
      my $Error = shift;
      log_message( "Error in CADC DB connectivity: $Error");
      log_message( "Skipping to next group.\n");
    }
    otherwise {
      my $Error = shift;
      $Error->throw;
    };
    if (!defined $recipe_id) {
      log_message( "Error submitting. No recipe id returned\n");
      last;
    } elsif ($recipe_id =~ /^\-/ ) {
      $recipe_id =~ s/^\-//;
      log_message( "*** Attempt to submit recipe update of $recipe_id with group $group failed.\n");
    } else {
      log_message( "Request submitted with recipe instance $recipe_id.\n");
      log_message( "Recipe URL: " . dprecinst_url($recipe_id) . "\n");
      log_message( "\n");
    }
  }

  if ($debug) {
    log_message( "Would be disconnecting from CADC data processing database here\n");
  } else {
    log_message( "Disconnecting from CADC data processing database...");
    disconnect_from_cadcdp( $dbh );
    log_message( "disconnected!\n\n");
  }
  log_message( "Data processing requests complete.\n");
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
