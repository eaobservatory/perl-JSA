package JSA::Submission;

=head1 NAME

JSA::Submission - Common routines for DP job submission scripts.

=cut

use OMP::ArcQuery;
use OMP::ArchiveDB;

use warnings;
use strict;

use parent qw/Exporter/;
our @EXPORT_OK = qw/prepare_archive_db/;

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
