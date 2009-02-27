package JSA::Headers::Starlink;

=head1 NAME

JSA::Headers::Starlink - Helper functions to deal with file headers
using Starlink tasks.

=head1 SYNOPSIS

  use JSA::Headers::Starlink;
  update_fits_headers( $file );

=head1 DESCRIPTION

This module provides helper functions that handle file headers for
NDFs.

=cut

use strict;
use warnings;
use warnings::register;

use File::Spec;

use JSA::Starlink qw/ check_star_env run_star_command /;

use Exporter 'import';
our @EXPORT_OK = qw/ update_fits_headers /;

=head1 FUNCTIONS

=over 4

=item B<update_fits_headers>

Update CADC-specific FITS headers in an NDF file.

  update_fits_headers( $file );

This function updates one FITS header:

 o INSTREAM: set to 'JCMT'

This function takes one argument: the NDF file to be updated.

This function does not return anything.

=cut

sub update_fits_headers {
  my $file = shift;

  # Make sure we actually need to do this. FITSMOD adds a new
  # card even if it already exists.

  check_star_env( "KAPPA", "fitsmod" );

  my @args = ( File::Spec->catfile( $ENV{KAPPA_DIR}, "fitsmod"),
               "NDF=$file",
               "KEYWORD=INSTREAM",
               "VALUE=JCMT",
               "COMMENT=\!",
               "EDIT=WRITE",
               "POSITION=\!" );

  run_star_command( @args );

}

=back

=head1  AUTHORS

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>,
Brad Cavanagh E<lt>b.cavanagh@jach.hawaii.eduE<gt>,

=head1 COPYRIGHT

Copyright (C) 2009 Science and Technology Facilities Council.
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
