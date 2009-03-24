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
use warnings;
use warnings::register;

use Carp;
use File::Copy;
use File::Spec;
use Proc::SafeExec;
use Starlink::Config qw/ :override /;
use Starlink::Versions qw/ starversion_lt starversion_string/;

use JSA::Error qw/ :try /;
use JSA::Headers qw/ update_fits_product read_header cadc_ack /;
use JSA::Headers::Starlink qw/ update_fits_headers add_fits_comments /;
use JSA::Starlink qw/ check_star_env run_star_command prov_update_parent_path
                      set_wcs_attribs /;
use JSA::Files qw/ drfilename_to_cadc cadc_to_drfilename
                   looks_like_drfile looks_like_cadcfile
                   can_send_to_cadc /;

use Exporter 'import';
our @EXPORT_OK = qw/ convert_to_fits convert_to_ndf convert_dr_files list_convert_plan /;

our $DEBUG = 0;

=head1 FUNCTIONS

=over 4

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

  # Read the header so that we can obtain the ASN_TYPE value
  my $hdr = read_header( $ndf );
  throw JSA::Error::DataRead( "Unable to read FITS header from $ndf" )
    unless defined $hdr;

  # Look for a ASN_TYPE header
  my $type = $hdr->value( "ASN_TYPE" );
  my $outfile;
  my $msg;
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
 - tempdir: a temporary directory for file conversion.

=cut

sub convert_dr_files {
  my $href = shift;

  my $opts = shift;

  for my $file ( sort keys %$href ) {

    if ( can_send_to_cadc( $href->{$file} ) &&
         looks_like_drfile( $file ) ) {

      print "Converting file $file\n" if $DEBUG;

      my $assoc = $href->{$file}->value( "ASN_TYPE" );

      # Copy the file to the temporary directory, if necessary.
      my $tfile = $file;
      if( defined( $opts->{tempdir} ) ) {
        $tfile = File::Spec->catfile( $opts->{tempdir}, $file );
        if( defined( $opts->{indir} ) ) {
          my $ifile = File::Spec->catfile( $opts->{indir}, $file );
          print "copying $ifile to $tfile\n" if $DEBUG;
          copy( $ifile, $tfile ) or die "Copy failed: $!";
        } else {
          copy( $file, $tfile ) or die "Copy failed: $!";
        }
      }

      # is exportable so first fix up provenance
      my $skip = 0;      
      try {
        prov_update_parent_path( $tfile );
      } catch JSA::Error with {
        # Just skip this file for now.
        my $E = shift;
        chomp($E);
        print "$E\n --- skipping\n";
        $skip = 1;
      };
      next if $skip;

      # Modify the WCS attributes so that we generate the correct FITS
      # headers regardless of how the pipeline was configured.
      set_wcs_attribs( $tfile );

      update_fits_headers( $tfile );

      my @comments = cadc_ack();
      add_fits_comments( $tfile, \@comments ) if @comments;

      # then convert to fits
      my $outfile = convert_to_fits( $tfile );

      # Now need to fix up PRODUCT names in extensions
      update_fits_product( $outfile );

      # At this point, the output file is in either the same directory
      # as the input file (if $opts->{tempdir} isn't defined) or in
      # the temporary directory (if $opts->{tempdir} is defined). If
      # we have been given an output directory, copy the output file
      # to a temporary filename in the output directory, then rename
      # it to the proper filename.
      if( defined( $opts->{outdir} ) ) {
        my $tempfilename = "cadc$$";
        my ( $vol, $dir, $ofile ) = File::Spec->splitpath( $outfile );
        copy( $outfile,
              File::Spec->catfile( $opts->{outdir}, $tempfilename ) );
        rename( File::Spec->catfile( $opts->{outdir}, $tempfilename ),
                File::Spec->catfile( $opts->{outdir}, $ofile ) );
        unlink( $outfile );
      }

      # Clean up temporary directory.
      if( defined( $opts->{tempdir} ) ) {
        unlink $tfile;
      }
    } else {
      if ($DEBUG) {
        my $can_send = can_send_to_cadc( $href->{$file} );
        my $isdr = looks_like_drfile( $file );
        print "File $file not suitable for conversion (is ".
          ( $can_send ? "" : "not ") . "valid product) (is ".
            ( $isdr ? "" : "not ") . "valid DR filename)\n";
      }
    }
  }
}

=item B<list_convert_plan>

Print to standard output information concerning which file will be converted
to FITS and which will be ignored. Does not guarantee that a file would be
converted successfully, just that it would be attempted.

 list_convert_plan( \%headers );

The only mandatory argument is a reference to a hash, keys being files
to be converted and values being an Astro::FITS::Header object created
from reading the header for the given filename. This is essentially a
reference to a hash as returned by the C<JSA::Headers->read_headers()>
method.

=cut

sub list_convert_plan {
  my $href = shift;

  my $opts = shift;

  for my $file ( sort keys %$href ) {

    if ( can_send_to_cadc( $href->{$file} ) &&
         looks_like_drfile( $file ) ) {
      my $assoc = $href->{$file}->value( "ASN_TYPE" );
      my $outfile = drfilename_to_cadc( $file, ASN_TYPE => $assoc );
      print "Converting file $file -> $outfile\n";
    } else {
      my $can_send = can_send_to_cadc( $href->{$file} );
      my $isdr = looks_like_drfile( $file );
      print "File $file not suitable for conversion (is ".
        ( $can_send ? "" : "not ") . "valid product) (is ".
          ( $isdr ? "" : "not ") . "valid DR filename)\n";
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
  my @args = ( File::Spec->catfile($StarConfig{"Star_Bin"},
                                   "convert", "ndf2fits"),
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


  print join(" ",@args)."\n" if $DEBUG;

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

  print join(" ",@args)."\n" if $DEBUG;

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
