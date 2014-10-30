package JSA::Headers::FITS;

=head1 NAME

JSA::Headers::FITS - Functions to deal with FITS file headers.

=head1 SYNOPSIS

  use JSA::Headers::FITS qw/update_fits_file_fits_headers/;
  update_fits_file_fits_headers($file, \%options);

=head1 DESCRIPTION

This module provides helper functions that handle file headers for
FITS files.

=cut

use strict;

use Astro::FITS::Header::CFITSIO;
use Astro::FITS::Header::Item;

use JSA::Headers::CADC qw/prepare_header_updates/;

=head1 FUNCTIONS

=over 4

=item update_fits_file_fits_headers

Update CADC-specific FITS headers in a FITS file.

This is the FITS file equivalent of the
JSA::Headers::Starlink::update_fits_headers function.

=cut

sub update_fits_file_fits_headers {
    my $file = shift;
    my $options = shift;

    my @updates = map {
        my ($header, $value, $comment) = @$_;
        new Astro::FITS::Header::Item(Keyword => $header,
                                      Value => $value,
                                      Comment => $comment);

    } @{prepare_header_updates($file, $options)};

    my $hdr = new Astro::FITS::Header::CFITSIO(File => $file);

    $hdr->append(\@updates);

    $hdr->writehdr(File => $file);
}

=back

=head1 COPYRIGHT

Copyright (C) 2014 Science and Technology Facilities Council.
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
