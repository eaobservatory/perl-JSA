package JSA::Convert;

=head1 NAME

JSA::Convert - Helper routines for converting FITS/NDF files for JSA

=head1 SYNOPSIS

  use JSA::Convert;

  $fits = convert_to_fits( $ndf );
  $ndf = convert_to_ndf( $fits );

=head1 DESCRIPTION

Helper routines for converting the supplied file into a format
suitable for the JCMT Science Archive (FITS format) or for pipeline
processing (NDF). The output file name will be derived from the
input name matching the standard scheme and using translation
routines provided by C<JSA::Files>.

=cut

use strict;
use Carp;
use warnings;
use File::Spec;
use warnings::register;
use Proc::SafeExec;
use Astro::FITS::Header;
use Astro::FITS::Header::NDF;

use Exporter 'import';
our @EXPORT_OK = qw( convert_to_fits convert_to_ndf convert_dr_files );

use Starlink::Versions qw/ starversion_lt starversion_string/;
use JSA::Error qw/ :try /;
use JSA::Headers qw/ update_fits_headers update_fits_product /;
use JSA::Starlink qw/ check_star_env run_star_command prov_update_parent_path
                      set_wcs_attribs /;
use JSA::Files qw/ drfilename_to_cadc cadc_to_drfilename
                   looks_like_drfile looks_like_cadcfile /;

# Products and associations to look for.
#our @PRODUCTS = qw/ reduced rimg rsp /;
#our @ASSOCS = qw/ obs night project public /;
our @PRODUCTS = qw/ /;
our @ASSOCS = qw/ obs /;
our %EXTRA_PRODUCTS = ( 'obs' => [ qw/ cube / ], );

# Set up a hash.
our %PRODS = map { $_ => { map { $_ => undef } @PRODUCTS } } @ASSOCS;
map { my $assoc = $_; map { $PRODS{$assoc}{$_} = undef } @{$EXTRA_PRODUCTS{$assoc}} } keys %EXTRA_PRODUCTS;

=head1 FUNCTIONS

=over 4

=item B<can_convert_to_fits>

Determine whether or not an NDF can be converted to a FITS file for
injest by CADC.

  $convert = can_convert_to_fits( $header );

A file can be converted if it is a science observation and its product
type is listed in the association type array.

The only argument is an C<Astro::FITS::Header> item created from the
NDF.

=cut

sub can_convert_to_fits {
  my $header = shift;

  return 0 if ( ! UNIVERSAL::isa( $header, "Astro::FITS::Header" ) );

  my $obstype = $header->value( "OBS_TYPE" );

  return 0 if ( ! defined $obstype || $obstype !~ /science/i );

  my $assoc = $header->value( "ASN_TYPE" );
  my $product = $header->value( "PRODUCT" );

  return 0 if( ! defined $assoc || ! defined $product );

  return 1 if ( exists $PRODS{$assoc}{$product} );

  return 0;

}

=item B<convert_to_fits>

Convert the supplied NDF to FITS.

  $fits = convert_to_fits( $ndf );

The current working directory is used for the conversion. Returns
undef if the input file name did not match the standard DR product
filenaming convention. An exception is thrown if the header can not be
read or if the conversion program failed.

Provenance will not be modified by this routine. The assumption
is that C<JSA::Prov> will have been used prior to conversion.
This may be an incorrect approach so at some point provenance
may be automatically called by this routine.

=cut

sub convert_to_fits {
  my $ndf = shift;

  # Do a quick check before we read the header
  return unless looks_like_drfile( $ndf );

  # Read the header so that we can obtain the ASN_TYPE
  # value
  my $hdr;
  my $msg;
  try {
    $hdr = Astro::FITS::Header::NDF->new( File => $ndf );
  } catch JSA::Error with {
    # should not be possible
    my $err = shift;
    $err->throw();
  } otherwise {
    my $E = shift;
    $msg = "$E";
  };
  throw JSA::Error::DataRead( "Unable to read FITS header from $ndf: $msg" )
    unless defined $hdr;

  # Look for a ASN_TYPE header
  my $type = $hdr->value( "ASN_TYPE" );
  my $outfile;
  try {
    $outfile = drfilename_to_cadc( $ndf, ASN_TYPE => $type );
  } catch JSA::Error with {
    my $E = shift;
    $E->throw;
  } otherwise {
    my $E = shift;
    $msg = "$E";
  };
  JSA::Error::FatalError->throw( "Unable to convert the DR filename ($ndf) to CADC form: $msg") if !defined $outfile;

  # Now do the conversion
  ndf2fits( $ndf, $outfile )
    or JSA::Error::Conversion->throw( "Could not convert $ndf to FITS" );

  return $outfile;
}

=item B<convert_to_ndf>

Convert the supplied FITS format CADC file into a form suitable
for processing in PiCARD.

  $ndf = convert_to_ndf( $fits );

Returns undef if the filename is not of the correct form. Throws
an exception if conversion fails.

Provenance is not modified by this routine.

=cut

sub convert_to_ndf {
  my $fits = shift;

  # convert the filename
  my $outfile = cadc_to_drfilename( $fits );
  return unless defined $outfile;

  # Do the conversion
  fits2ndf( $fits, $outfile)
    or JSA::Error::Conversion->throw( "Could not convert $fits to NDF" );

  return $outfile; 
}

=item B<convert_dr_files>

Convert a list of ORAC-DR-created files into FITS files in preparation
for injest by CADC.

  convert_dr_files( $hashref, \%options );

The only mandatory argument is a reference to a hash, keys being files
to be converted and values being an Astro::FITS::Header object created
from reading the header for the given filename. This is essentially a
reference to a hash as returned by the C<JSA::Headers->read_headers()>
method.

Optional arguments are passed in a hash reference with the following
allowed keys:

 - indir: the input directory
 - outdir: the output directory

=cut

sub convert_dr_files {
  my $href = shift;

  my $opts = shift;

  for my $file ( sort keys %$href ) {
    if ( can_convert_to_fits( $href->{$file} ) ) {

      my $assoc = $href->{$file}->value( "ASN_TYPE" );

      if( defined( $opts->{indir} ) ) {
        $file = File::Spec->catfile( $opts->{indir}, $file );
      }

      # is exportable so first fix up provenance
      prov_update_parent_path( $file, keys %{$PRODS{$assoc}} );

      # Modify the WCS attributes so that we generate the correct FITS
      # headers regardless of how the pipeline was configured.
      set_wcs_attribs( $file );

      update_fits_headers( $file );

      # then convert to fits
      my $outfile = convert_to_fits( $file );

      # Now need to fix up PRODUCT names in extensions
      update_fits_product( $outfile );

      # Rename the file if the input directory isn't the same as the
      # output directory.
      if( defined( $opts->{outdir} ) &&
          defined( $opts->{indir} ) &&
          $opts->{indir} ne $opts->{outdir} ) {
        my ( $vol, $dir, $ofile ) = File::Spec->splitpath( $outfile );
        rename( $outfile,
                File::Spec->catfile($opts->{outdir}, $ofile ) );
      }
    }
  }
}

=back

=begin PRIVATE__SUBS

=head1 PRIVATE FUNCTIONS

=over 4

=item B<ndf2fits>

Convert NDF to FITS format using NDF2FITS command.

  ndf2fits( $infile, $outfile )
    or die "Error converting to fits";

=cut

# Convert to fits: infile, outfile
sub ndf2fits {
  my $infile = shift;
  my $outfile = shift;

  # make sure we have a reasonable environment
  check_star_env( "CONVERT", "ndf2fits" );

  # Remove the output file before we start
  unlink $outfile if -e $outfile;

  my $has_cadc_prov = 1;
  if (starversion_lt('convert', '1.5-13') ) {
    my $ver = starversion_string("convert");
    carp "CADC provenance is not supported by this version of CONVERT NDF2FITS ($ver)."
      ." Please upgrade to at least v1.5-13.\n";
    $has_cadc_prov = 0;
  }

  # CADC specific options
  my @args = ( File::Spec->catfile($ENV{CONVERT_DIR}, "ndf2fits"),
               "IN=$infile",
               "OUT=$outfile",
               "ENCODING=FITS-WCS(CD)",
               "CHECKSUM",
               "PROEXTS",
               "PROFITS",
               "DUPLEX",
               "PROHIS",
               ($has_cadc_prov ? "PROVENANCE=CADC" : () ),
               "COMP=DV" );


  print join(" ",@args)."\n";

  # consider catching the BadExec error
  run_star_command( @args );
  return 1;
}

=item B<fits2ndf>

Run CONVERT fits2ndf to convert the supplied fits file to NDF.

  fits2ndf( $infile, $outfile ) or die "Could not convert";

=cut

sub fits2ndf {
  my $infile = shift;
  my $outfile = shift;

  # make sure we have a reasonable environment
  check_star_env( "CONVERT", "fits2ndf" );

  # Remove the output file before we start
  unlink $outfile if -e $outfile;

  # CADC specific options
  my @args = ( File::Spec->catfile($ENV{CONVERT_DIR}, "fits2ndf"),
               "IN=$infile",
               "OUT=$outfile",
             );

  print join(" ",@args)."\n";

  # consider catching the BadExec error
  run_star_command( @args );
  return 1;
}

=back

=end PRIVATE__SUBS

=head1 AUTHORS

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>,
Brad Cavanagh E<lt>b.cavanagh@jach.hawaii.eduE<gt>

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
