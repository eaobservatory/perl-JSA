package JSA::Headers;

=head1 NAME

JSA::Headers - Helper functions to deal with file headers.

=head1 SYNOPSIS

  use JSA::Headers;
  update_fits_product( $fits_file );

=head1 DESCRIPTION

This module provides helper functions that handle file headers for
both NDFs and FITS files.

=cut

use strict;
use warnings;
use warnings::register;

use Astro::FITS::CFITSIO;
use Astro::FITS::HdrTrans;
use Astro::FITS::Header::NDF;
use Carp;
use NDF 1.47;
use Starlink::Config qw/ :override /;

use JSA::Files qw/ drfilename_to_cadc /;
use JSA::Starlink qw/ check_star_env run_star_command /;

use Exporter 'import';
our @EXPORT_OK = qw/ read_headers get_header_value get_orac_instrument
                     update_fits_headers update_fits_product /;

=head1 FUNCTIONS

=over 4

=item B<get_header_value>

Retrieve number of values matching a given FITS header keyword.

  my ( $number, %values ) = get_header_value( $key, @headers );

This function takes two arguments: the keyword to search for and a
list of Astro::FITS::Header objects.

In returning, this function returns the number of header items that
match the requested keyword across all given headers, and a hash with
keys being the value of the FITS header matching the requested
keyword, and values being the number of times that given value
matched.

=cut

sub get_header_value {
  my ($key, @hdrs) = @_;
  my $nhits = 0;
  my %values;
  for my $h (@hdrs) {
    my $value = $h->value($key);
    if (defined $value) {
      $values{$value}++;
      $nhits++;
    }
  }
  return ($nhits, %values);
}

=item B<get_orac_instrument>

Determine the instrument for a given set of headers. This instrument
can then be used to initialize ORAC-DR.

  my $instrument = get_orac_instrument( $header );

This function looks at two generic headers as returned by
Astro::FITS::HdrTrans: INSTRUMENT and BACKEND. If the BACKEND is
'ACSIS', 'DAS', or 'AOSC', then the returned instrument is 'ACSIS'. If
the INSTRUMENT is 'SCUBA2, then the SUBARRAY header is examined -- if
it begins with 's8' then the returned instrument is 'SCUBA2_LONG', and
if it begins with 's4' the returned instrument is 'SCUBA2_SHORT'. In
all other cases, the returned instrument is the same as the INSTRUMENT
generic header.

The only argument must be an Astro::FITS::Header object. If the
instrument does not have header translation code defined by the
Astro::FITS::HdrTrans module, this method will croak.

Returns a string.

=cut

sub get_orac_instrument {
  my $hdr = shift;

  # Make sure the given $hdr is an Astro::FITS::Header object.
  if( ! UNIVERSAL::isa( $hdr, "Astro::FITS::Header" ) ) {
    croak "Input to get_orac_instrument must be an Astro::FITS::Header object";
  }

  # We need the INSTRUMENT and the BACKEND
  my %fits;
  tie %fits, "Astro::FITS::Header", $hdr;
  my $class = Astro::FITS::HdrTrans::determine_class( \%fits, undef, 1);

  if( ! defined $class ) {
    croak "Unable to determine header translation class";
  }

  my $instrument = $class->to_INSTRUMENT( \%fits );
  my $backend = $class->to_BACKEND( \%fits );

  my $oa;
  if ($backend eq 'ACSIS' || $backend eq 'DAS' || $backend eq 'AOSC') {
    $oa = "ACSIS";
  } elsif ($instrument eq 'SCUBA2') {
    # depends on long vs short
    my $subarray = $hdr->value("SUBARRAY");
    if (defined $subarray) {
      if ($subarray =~ /^s8/) {
        $oa = "SCUBA2_LONG";
      } elsif ($subarray =~ /^s4/) {
        $oa = "SCUBA2_SHORT";
      }
    }
  } else {
    # go with instrument
    $oa = $instrument;
  }

  return $oa;
}

=item B<read_headers>

Read headers from a list of files.

  my %headers = read_headers( @files );

Returns a hash, keys being the filename and values being an
Astro::FITS::Header object created from reading the header for the
given filename.

=cut

sub read_headers {
  my @files = @_;

  my %headers;
  for my $f (@files) {
    my $hdr;
    if ($f =~ /\.f.*$/) {
      $hdr = eval { Astro::FITS::Header::CFITSIO->new( File => $f )};
    } else {
      $hdr = eval { Astro::FITS::Header::NDF->new( File => $f )};
    }
    $headers{$f} = $hdr if defined $hdr;
  }

  return %headers;
}

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

  check_star_env( "KAPPA", "fitsmod" );

  my @args = ( File::Spec->catfile( $StarConfig{"Star_Bin"},
                                    "kappa", "fitsmod" ),
               "NDF=$file",
               "KEYWORD=INSTREAM",
               "VALUE=JCMT",
               "COMMENT=\!",
               "EDIT=WRITE",
               "POSITION=\!" );

  run_star_command( @args );

}

=item B<update_fits_product>

Update CADC-specific FITS headers in a FITS file.

  update_fits_product( $file );

This function updates one FITS header:

 o PRODUCT

This function takes one argument: the FITS file to be updated.

This function does not return anything.

=cut

sub update_fits_product {
  my $file = shift;

  my $status = 0;
  my $ifits = Astro::FITS::CFITSIO::open_file( $file, Astro::FITS::CFITSIO::READWRITE(), $status );

  $ifits->get_num_hdus( my $numhdus, $status );

  # we only have to modify extensions
  if ($numhdus > 1) {
    # Read PRODUCT from PRIMARY header
    $ifits->read_key( Astro::FITS::CFITSIO::TSTRING(), "PRODUCT", my $prodref, my $pcomment, $status );
    for my $i (2..$numhdus) {
      last if $status != 0;
      $ifits->movabs_hdu( $i, my $hdutype, $status );
      next unless $hdutype == Astro::FITS::CFITSIO::IMAGE_HDU();

      # Get the EXTNAME
      $ifits->read_key( Astro::FITS::CFITSIO::TSTRING(), "EXTNAME", my $extname, undef, $status );
      if ($status != 0) {
        $status = 0;
        next;
      }

      # Need thing after last dot
      $extname = (split(/\./,$extname))[-1];

      # set the new value for use PRODUCT (lower case version of extension)
      my $newprod = $prodref . lc("_$extname");

      $ifits->update_key( Astro::FITS::CFITSIO::TSTRING(), "PRODUCT", $newprod, undef, $status );
      $status = 0;
    }
  }

  $ifits->close_file( $status );

}

=back

=head1 AUTHORS

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>,
Brad Cavanagh E<lt>b.cavanagh@jach.hawaii.eduE<gt>,

=head1 COPYRIGHT

Copyright (C) 2008 Science and Technology Facilities Council.
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
