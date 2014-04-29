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
use Image::ExifTool qw/ :Public /;
use Proc::SafeExec;
use File::Basename qw/ fileparse /;
use Starlink::Config qw/ :override /;
use Starlink::Versions qw/ starversion_lt starversion_string/;

use JSA::Error qw/ :try /;
use JSA::Headers qw/ update_fits_product read_header cadc_ack /;
use JSA::Headers::CADC qw/ correct_asn_id /;
use JSA::Headers::Starlink qw/ update_fits_headers add_fits_comments /;
use JSA::Starlink qw/ check_star_env run_star_command prov_update_parent_path
                      set_wcs_attribs /;
use JSA::Files qw/ drfilename_to_cadc cadc_to_drfilename
                   looks_like_drfile looks_like_cadcfile
                   can_send_to_cadc looks_like_drthumb
                   merge_pngs want_to_send_to_cadc /;

use Exporter 'import';
our @EXPORT_OK = qw/ convert_to_fits convert_to_ndf
                     convert_dr_files list_convert_plan
                     ndf2fits /;

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
for ingest by CADC.

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
 - mode: Processing mode ("obs", "night", "project", "public").
 - dpid: Recipe instance ID for data processing
 - dpdate: ISO8601 date to assign to each converted file

=cut

sub convert_dr_files {
  my $href = shift;

  my $opts = shift;
  my $mode = $opts->{'mode'};
  my $dpid = $opts->{dpid};
  my $dpdate = $opts->{dpdate};

  my @pngs;

  for my $file ( sort keys %$href ) {

    if( looks_like_drfile( $file ) ) {

      if ( can_send_to_cadc( $href->{$file} ) && want_to_send_to_cadc( $mode, header => $href->{$file} ) ) {

        print "Converting file $file\n" if $DEBUG;

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

        update_fits_headers( $tfile, { "mode" => $mode,
                                       "dpid" => $dpid,
                                       "dpdate" => $dpdate, } );

        my @comments = cadc_ack();
        add_fits_comments( $tfile, \@comments ) if @comments;

        # then convert to fits
        my $outfile = convert_to_fits( $tfile );

        # Now need to fix up PRODUCT names in extensions
        update_fits_product( $outfile );

        # At this point, the output file is in either the same
        # directory as the input file (if $opts->{tempdir} isn't
        # defined) or in the temporary directory (if $opts->{tempdir}
        # is defined). If we have been given an output directory, copy
        # the output file to a temporary filename in the output
        # directory, then rename it to the proper filename.
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
          my $want = want_to_send_to_cadc( $mode, header => $href->{$file} );
          print "File $file not suitable for conversion (is ".
            ( $can_send ? "" : "not ") . "valid product) (is ".
            ( $want ? "" : "not ") . "wanted at CADC)\n";
        }

      }
    } elsif( looks_like_drthumb( $file ) && want_to_send_to_cadc( $mode, filename => $file ) ) {

      print "Converting file $file\n" if $DEBUG;

      my $outfile = rename_png( $file, { "mode" => $mode } );
      write_dpinfo_png( $outfile, $dpdate, $dpid );
      push @pngs, $outfile;

    } else {
      if ($DEBUG) {
        my $can_send = can_send_to_cadc( $href->{$file} );
        my $isdr = looks_like_drfile( $file );
        my $want = want_to_send_to_cadc( $mode, filename => $href->{$file} );
        print "File $file not suitable for conversion (is ".
          ( $can_send ? "" : "not ") . "valid product) (is ".
          ( $isdr ? "" : "not ") . "valid DR filename) (is ".
          ( $want ? "" : "not " ) . "wanted at CADC)\n";
      }
    }
  }

  # And merge the PNGs we've created.
  my $reduced = merge_pngs( @pngs );

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

    if( looks_like_drfile( $file ) ) {
      if ( can_send_to_cadc( $href->{$file} ) ) {
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
    } elsif( looks_like_drthumb( $file ) ) {
      my $outfile = drfilename_to_cadc( $file );
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

=item B<rename_png>

Rename a PNG as created by ORAC-DR to the filename convention for CADC
ingest. Returns the name of the copied PNG.

 $pngout = rename_png( $pngin, \%options );

Where the options hash can include:

 - mode: Processing mode ("obs", "night", "project", "public").

The JSA naming convention for preview images is

 JCMT_<asn_id>_<product_id>_preview_SIZE.png

where <asn_id> is the observation identifier in frame products
and is the association identifier for group products. The
product ID is a combination of the product type and
subsystem number.

Note that the ASN_ID value stored in the header is the
base value and should be modified depending on the
processing mode (night, project, public). See
C<JSA::Headers::CADC::correct_asn_id> for more
information.

All this informtation is read from the header of the
input PNG.

=cut

sub rename_png {
  my $infile = shift;
  my $opts = shift;

  my $mode = $opts->{'mode'};

  # Sanity check
  JSA::Error::BadArgs->throw( "Supplied PNG: '$infile' does not exist")
      unless -e $infile;

  # Check the suffix
  my @file_parsed = fileparse($infile, qw/ .png .jpg .jpeg /);
  my $suffix = $file_parsed[2];
  JSA::Error::BadArgs->throw( "Input file ($infile) must be a PNG file" )
      unless $suffix =~ /\.png/i;

  # Read the EXIF header to find out what type of ASN_TYPE we have.
  my $exif = new Image::ExifTool;
  $exif->ExtractInfo( $infile );
  my @keywords = $exif->GetValue('Keywords');
  my %keywords = map { split '=', $_ } @keywords;

  # Observation or group mode
  my $assoc = $keywords{'jsa:asn_type'};

  if( defined( $assoc ) && $assoc eq 'night' ) {
    $assoc = $mode;
  }

  my $outfile;
  my $asn_id;
  if( defined( $assoc ) ) {
    # Use the association ID unless obs
    if ($assoc =~ /^obs/i) {
      $asn_id = $keywords{"jsa:obsid"};
    } else {
      # Convert the ASN_ID to unique form
      # First need to convert jsa: headers to a hash
      # like a FITS header
      my %hdr;
      for my $k (keys %keywords) {
        if ($k =~ /^jsa:(.*)$/) {
          $hdr{uc($1)} = $keywords{$k};
        }
      }
      $asn_id = correct_asn_id( $assoc, \%hdr );
    }
  } else {
    # Assume OBS product
    $asn_id = $keywords{"jsa:obsid"};
  }

  my $productID = $keywords{'jsa:productID'};

  JSA::Error::BadFITSHeader->throw("jsa:productID header is missing from PNG file $infile")
      unless defined $productID;

  my $size = $exif->GetValue( "ImageHeight" );

  $outfile = join("_", "JCMT", $asn_id, $productID, "preview", $size ). $suffix;

  copy( $infile, $outfile );

  return $outfile;

}

=item B<write_dpinfo_png>

Write data processing information to the PNG EXIF
header.

  write_dpinfo_png( $png, $dpdate, $dpid );

No action if the data processing date or ID are undef.

=cut

sub write_dpinfo_png {
  my $png = shift;
  my $dpdate = shift;
  my $dpid = shift;
  return if (!defined $dpdate && !defined $dpid);

  my $exif = Image::ExifTool->new();

  # We have to read all the info in first
  $exif->ExtractInfo( $png );
  my @keywords = $exif->GetValue( 'Keywords' );

  # and write it out
  for my $k (@keywords) {
    $exif->SetNewValue( Keywords => $k );
  }

  # and the new stuff
  $exif->SetNewValue( Keywords => "jsa:dpdate=$dpdate" )
    if defined $dpdate;
  $exif->SetNewValue( Keywords => "jsa:dprcinst=$dpid" )
    if defined $dpid;
  $exif->WriteInfo( $png );
}

=item B<ndf2fits>

Convert NDF to FITS format using NDF2FITS command.

  ndf2fits( $infile, $outfile )
    or die "Error converting to fits";

This is a low level subroutine.  JSA software should generally
call the convert_to_fits wrapper subroutine instead.

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

=back

=begin PRIVATE__SUBS

=head1 PRIVATE FUNCTIONS

=over 4

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
