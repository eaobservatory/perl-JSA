package JSA::Submission;

=head1 NAME

JSA::Submission - Common routines for DP job submission scripts.

=cut

use File::Basename;

use OMP::ArcQuery;
use OMP::ArchiveDB;
use JSA::Headers::CADC qw/correct_asn_id/;

use warnings;
use strict;

use parent qw/Exporter/;
our @EXPORT_OK = qw/assign_to_group get_obsidss prepare_archive_db/;

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
