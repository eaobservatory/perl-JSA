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
use Image::ExifTool qw/:Public/;
use Proc::SafeExec;
use File::Basename qw/fileparse/;
use Starlink::Config qw/:override/;

use JSA::Error qw/:try/;
use JSA::Headers qw/update_fits_product read_header cadc_ack/;
use JSA::Headers::CADC qw/correct_asn_id/;
use JSA::Headers::FITS qw/update_fits_file_fits_headers/;
use JSA::Headers::Starlink qw/update_fits_headers add_fits_comments/;
use JSA::Starlink qw/check_star_env run_star_command prov_update_parent_path
                     set_wcs_attribs get_ndf_bb/;
use JSA::Files qw/drfilename_to_cadc cadc_to_drfilename
                  looks_like_drfile looks_like_fits_drfile looks_like_cadcfile
                  looks_like_rawfile can_send_to_cadc_guess
                  can_send_to_cadc looks_like_drthumb
                  dissect_drfile construct_rawfile
                  merge_pngs want_to_send_to_cadc/;

use Exporter 'import';
our @EXPORT_OK = qw/convert_to_fits convert_to_ndf
                    convert_dr_files list_convert_plan
                    ndf2fits write_dpinfo_png/;

our $DEBUG = 0;

=head1 FUNCTIONS

=over 4

=item B<convert_to_fits>

Convert the supplied NDF to FITS.

  $fits = convert_to_fits( $ndf, $version );

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
    my $version = shift;

    # Do a quick check before we read the header
    return unless looks_like_drfile($ndf);

    # Read the header so that we can obtain the ASN_TYPE value
    my $hdr = read_header($ndf);
    throw JSA::Error::DataRead("Unable to read FITS header from $ndf")
      unless defined $hdr;

    # Look for a ASN_TYPE header
    my $type = $hdr->value("ASN_TYPE");
    my $outfile;
    my $msg;

    try {
        $outfile = drfilename_to_cadc($ndf, ASN_TYPE => $type,
                                      VERSION => $version);
    }
    catch JSA::Error with {
        my $E = shift;
        $E->throw;
    }
    otherwise {
        my $E = shift;
        $msg = "$E";
    };

    JSA::Error::FatalError->throw("Unable to convert the DR filename ($ndf) to CADC form: $msg")
        unless defined $outfile;

    # Now do the conversion
    my %opt = (quality => 1);

    $opt{'no_provenance'} = 1 if $type eq 'public';

    ndf2fits($ndf, $outfile, %opt)
        or JSA::Error::Conversion->throw( "Could not convert $ndf to FITS" );

    return $outfile;
}

=item B<convert_to_ndf>

Convert the supplied FITS format CADC file into a form suitable
for processing in PiCARD.

    $ndf = convert_to_ndf($fits);

Returns undef if the filename is not of the correct form. Throws
an exception if conversion fails.

Provenance is not modified by this routine.

=cut

sub convert_to_ndf {
    my $fits = shift;

    # convert the filename
    my $outfile = cadc_to_drfilename($fits);
    return unless defined $outfile;

    # Do the conversion
    fits2ndf($fits, $outfile)
        or JSA::Error::Conversion->throw("Could not convert $fits to NDF");

    return $outfile;
}

=item B<convert_dr_files>

Convert a list of ORAC-DR-created files into FITS files in preparation
for ingest by CADC.

    convert_dr_files($hashref, \%options);

The only mandatory argument is a reference to a hash, keys being files
to be converted and values being an Astro::FITS::Header object created
from reading the header for the given filename. This is essentially a
reference to a hash as returned by the C<JSA::Headers->read_headers()>
method.

Optional arguments are passed in a hash reference with the following
allowed keys:

 - indir: the input directory
 - mode: Processing mode ("obs", "night", "project", "public").
 - dpid: Recipe instance ID for data processing
 - dpdate: ISO8601 date to assign to each converted file
 - version: file version number (for "public" mode files only)
 - dry_run: return hash of conversions which would be done
            rather than actually doing anything

=cut

sub convert_dr_files {
    my $href = shift;

    my $opts = shift;
    my $mode = $opts->{'mode'};
    my $dpid = $opts->{dpid};
    my $dpdate = $opts->{dpdate};
    my $version = $opts->{'version'};
    my $dry_run = $opts->{'dry_run'};

    my @pngs;

    my %fits_options = (
        mode => $mode,
        dpid => $dpid,
        dpdate => $dpdate,
    );

    my %conversion = ();

    foreach my $file (sort keys %$href) {
        if (looks_like_drfile($file)) {
            if (can_send_to_cadc($mode, $href->{$file}) && want_to_send_to_cadc($mode, header => $href->{$file})) {
                print "Converting file $file\n" if $DEBUG;

                # is exportable so first fix up provenance
                my $skip = 0;
                try {
                    prov_update_parent_path($file,
                                            \&looks_like_drfile,
                                            \&looks_like_cadcfile,
                                            \&_prov_check_file,
                                            _prov_convert_filename($version),
                                            strict_check => 0,
                                            dry_run => $dry_run);
                }
                catch JSA::Error with {
                    # Just skip this file for now.
                    my $E = shift;
                    chomp($E);
                    print "$E\n --- skipping file $file\n";
                    $skip = 1;
                };
                next if $skip;

                if ($dry_run) {
                    my $assoc = $href->{$file}->value("ASN_TYPE");
                    $conversion{$file} = drfilename_to_cadc(
                        $file,
                        ASN_TYPE => (($assoc eq 'obs') ? $assoc : $mode),
                        VERSION => (($mode eq 'public') ? $version : undef));
                    next;
                }

                # Modify the WCS attributes so that we generate the correct FITS
                # headers regardless of how the pipeline was configured.
                set_wcs_attribs($file);

                update_fits_headers($file, \%fits_options);

                my @comments = cadc_ack();
                add_fits_comments($file, \@comments) if @comments;

                # then convert to fits
                my $outfile = convert_to_fits(
                    $file, (($mode eq 'public') ? $version : undef));

                # Now need to fix up PRODUCT names in extensions
                update_fits_product($outfile);
            }
            else {
                if ($DEBUG) {
                    my $can_send = can_send_to_cadc($mode, $href->{$file});
                    my $want = want_to_send_to_cadc($mode, header => $href->{$file});
                    print "File $file not suitable for conversion (is "
                        . ($can_send ? "" : "not ") . "valid product) (is "
                        . ($want ? "" : "not ") . "wanted at CADC)\n";
                }
            }
        }
        elsif (looks_like_fits_drfile($file) and
               can_send_to_cadc($mode, $href->{$file}) and
               want_to_send_to_cadc($mode, header => $href->{$file})) {
            print "Processing FITS file $file\n" if $DEBUG;

            if ($dry_run) {
                my $assoc = $href->{$file}->value("ASN_TYPE");
                $conversion{$file} = drfilename_to_cadc(
                    $file,
                    ASN_TYPE => (($assoc eq 'obs') ? $assoc : $mode),
                    VERSION => (($mode eq 'public') ? $version : undef));
                next;
            }

            update_fits_file_fits_headers($file, \%fits_options);

            my $hdr = read_header($file);
            my $type = $hdr->value('ASN_TYPE');
            my $outfile = drfilename_to_cadc(
                $file, ASN_TYPE => $type,
                VERSION => (($mode eq 'public') ? $version : undef));

            copy($file, $outfile);

        }
        elsif( looks_like_drthumb($file) && want_to_send_to_cadc($mode, filename => $file)) {
            print "Converting file $file\n" if $DEBUG;

            my ($outfile, $asn_id) = rename_png($file, {
                mode => $mode,
                version => (($mode eq 'public') ? $version : undef),
                dry_run => $dry_run,
            });

            if ($dry_run) {
                $conversion{$file} = $outfile;
                next;
            }

            write_dpinfo_png($outfile, $dpdate, $dpid, asn_id => $asn_id);
            push @pngs, $outfile;

        }
        else {
            if ($DEBUG) {
                my $can_send = can_send_to_cadc($mode, $href->{$file});
                my $isdr = looks_like_drfile($file);
                my $isfitsdr = looks_like_fits_drfile($file);
                my $want = want_to_send_to_cadc($mode, filename => $href->{$file});
                print "File $file not suitable for conversion (is "
                    . ($can_send ? "" : "not ") . "valid product) (is "
                    . ($isdr ? "" : "not ") . "valid DR filename) (is "
                    . ($isfitsdr ? "" : "not ") . "valid FITS DR filename) (is "
                    . ($want ? "" : "not ") . "wanted at CADC)\n";
            }
        }
    }

    return %conversion if $dry_run;

    # And merge the PNGs we've created.
    my $reduced = merge_pngs(@pngs);
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
    my $mode = $opts->{'mode'};
    my $version = $opts->{'version'};

    my %conversion = convert_dr_files(
        $href, {mode => $mode, version => $version, dry_run => 1});

    foreach (sort keys %conversion) {
        printf "Converting file %s -> %s\n", $_, $conversion{$_};
    }
}

=item B<rename_png>

Rename a PNG as created by ORAC-DR to the filename convention for CADC
ingest. Returns the name of the copied PNG and ASN_ID, if any.

   ($pngout, $asn_id) = rename_png($pngin, \%options);

Where the options hash can include:

   - mode: Processing mode ("obs", "night", "project", "public").

Note that the ASN_ID value stored in the header is the
base value so this function also returns
a value modified depending on the
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
    my $version = $opts->{'version'};

    # Sanity check
    JSA::Error::BadArgs->throw("Supplied PNG: '$infile' does not exist")
        unless -e $infile;

    # Check the suffix
    my @file_parsed = fileparse($infile, qw/ .png .jpg .jpeg /);
    my $suffix = $file_parsed[2];
    JSA::Error::BadArgs->throw("Input file ($infile) must be a PNG file")
        unless $suffix =~ /\.png/i;

    # Read the EXIF header to find out what type of ASN_TYPE we have.
    my $exif = new Image::ExifTool;
    $exif->ExtractInfo( $infile );
    my @keywords = $exif->GetValue('Keywords');
    my %keywords = map {split '=', $_} @keywords;

    # Observation or group mode
    my $assoc = $keywords{'jsa:asn_type'};

    if (defined( $assoc ) && $assoc eq 'night') {
        $assoc = $mode;
    }

    my $outfile;
    my $asn_id;

    if (defined($assoc)) {
        # Use the association ID unless obs
        if ($assoc !~ /^obs/i) {
            # Convert the ASN_ID to unique form
            # First need to convert jsa: headers to a hash
            # like a FITS header
            my %hdr;

            foreach my $k (keys %keywords) {
                if ($k =~ /^jsa:(.*)$/) {
                    $hdr{uc($1)} = $keywords{$k};
                }
            }

            $asn_id = correct_asn_id($assoc, \%hdr);
        }
    }

    $outfile = drfilename_to_cadc(
        $infile, ASN_TYPE => $assoc, VERSION => $version);

    copy($infile, $outfile) unless $opts->{'dry_run'};

    return ($outfile, $asn_id);

}

=item B<write_dpinfo_png>

Write data processing information to the PNG EXIF
header.

    write_dpinfo_png($png, $dpdate, $dpid, %opt);

Extra options:

    asn_id - new value for jsa:asn_id keyword

No action if the data processing date and ID and asn_id are undef.

=cut

sub write_dpinfo_png {
    my $png = shift;
    my $dpdate = shift;
    my $dpid = shift;
    my %opt = @_;

    my $asn_id = $opt{'asn_id'};
    return if (!defined $dpdate && !defined $dpid && !defined $asn_id);

    my $exif = Image::ExifTool->new();

    # We have to read all the info in first
    $exif->ExtractInfo($png);
    my @keywords = $exif->GetValue('Keywords');

    $exif->SetNewValue(Keywords => 'jsa:asn_id=' . $asn_id)
        if defined $asn_id;

    # and write it out
    foreach my $k (@keywords) {
        next if $k =~ /^jsa:asn_id/ and defined $asn_id;

        $exif->SetNewValue(Keywords => $k);
    }

    # and the new stuff
    $exif->SetNewValue(Keywords => "jsa:dpdate=$dpdate")
        if defined $dpdate;
    $exif->SetNewValue(Keywords => "jsa:dprcinst=$dpid")
        if defined $dpid;
    $exif->WriteInfo($png);
}

=item B<ndf2fits>

Convert NDF to FITS format using NDF2FITS command.

    ndf2fits($infile, $outfile)
        or die "Error converting to fits";

This is a low level subroutine.  JSA software should generally
call the convert_to_fits wrapper subroutine instead.

=cut

# Convert to fits: infile, outfile
sub ndf2fits {
    my $infile = shift;
    my $outfile = shift;
    my %opt = @_;

    # make sure we have a reasonable environment
    check_star_env("CONVERT", "ndf2fits");

    # Remove the output file before we start
    unlink $outfile if -e $outfile;

    my $comp = 'DV';
    if ($opt{'quality'}) {
        # If outputting a quality array, ensure that the bad-bit mask
        # is zero, otherwise FITS viewers which only look at the primary
        # HDU won't realize that some pixels are bad.
        JSA::Error::Conversion->throw('File ' . $infile . ' has non-zero BB mask')
            unless get_ndf_bb($infile) == 0;
        $comp .= 'Q';
    }

    # CADC specific options
    my @args = (
        File::Spec->catfile($StarConfig{"Star_Bin"}, "convert", "ndf2fits"),
        "IN=$infile",
        "OUT=$outfile",
        "ENCODING=FITS-WCS(CD)",
        'AXISORDER=COPY',
        "CHECKSUM",
        "PROEXTS",
        "PROFITS",
        "DUPLEX",
        "PROHIS",
        "PROVENANCE=CADC",
        "COMP=$comp",
    );

    push @args, 'PROPROV=NO' if $opt{'no_provenance'};

    print join(" ",@args)."\n" if $DEBUG;

    # consider catching the BadExec error
    run_star_command(@args);
    return 1;
}

=back

=begin PRIVATE__SUBS

=head1 PRIVATE FUNCTIONS

=over 4

=item B<fits2ndf>

Run CONVERT fits2ndf to convert the supplied fits file to NDF.

    fits2ndf($infile, $outfile) or die "Could not convert";

=cut

sub fits2ndf {
    my $infile = shift;
    my $outfile = shift;

    # make sure we have a reasonable environment
    check_star_env("CONVERT", "fits2ndf");

    # Remove the output file before we start
    unlink $outfile if -e $outfile;

    # CADC specific options
    my @args = (
        File::Spec->catfile($ENV{CONVERT_DIR}, "fits2ndf"),
        "IN=$infile",
        "OUT=$outfile",
    );

    print join(" ", @args)."\n" if $DEBUG;

    # consider catching the BadExec error
    run_star_command(@args);
    return 1;
}

=item B<_prov_check_file>

Determine whether a file is one which we would like to keep in
the provenance or not.

=cut

sub _prov_check_file {
    my $path = shift;

    if (looks_like_cadcfile($path)) {
        # assume that if the provenance already includes CADC form
        # that this file is okay
        print "Looks like CADCFILE\n" if $DEBUG;
        return 1;

    }
    elsif (looks_like_drfile($path)) {
        # Reading the header may take a lot longer than parsing the
        # filename but for now we do that since that is required
        # if we do not wish to reimplement the logic in can_send_to_cadc.
        print "Looks like DR ($path)\n" if $DEBUG;

        # in some cases intermediate files have been deleted even
        # though they match the dr file name test. We use a quick
        # test to see if they are close to being relevant and if they
        # are relevant we do an additional test with the header
        if (can_send_to_cadc_guess($path)) {
            print "Can possibly send $path\n" if $DEBUG;

            # Check the file exists before trying to read its header.
            unless (-e $path) {
                print "File does not exist: $path\n" if $DEBUG;
                return 0;
            }

            # Open up the header, send it to can_send_to_cadc() to find
            # out if this file is a suitable one to send to CADC.
            my $hdr = read_header($path);
            unless (defined $hdr) {
                print "Unable to read FITS header from $path\n" if $DEBUG;
                return 0;
            }
            elsif (can_send_to_cadc(undef, $hdr)) {
                # we are good
                print "Product match\n" if $DEBUG;
                return 1;
            }
        }
    }
    elsif (looks_like_rawfile($path)) {
        print "Looks like raw\n" if $DEBUG;
        return 1;
    }

    print "$path not CADC, DR or raw file\n" if $DEBUG;
    return 0;
}

=item B<_prov_convert_filename>

Return a reference to a subroutine which can be used to
convert the name of a file in the provenance system into the
corresponding CADC filename which we would like to be the
new provenance entry.

    prov_update_parent_path(..., _prov_convert_filename($version), ...);

This is implemented as a closure so that it can retain
access to the file version.

=cut

sub _prov_convert_filename {
    my $version = shift;
    return sub {
        my $path = shift;
        my $basedir = shift;
        my $haspar = shift;

        # Check to see if this file exists. If it doesn't, we'll check in the
        # same directory as the original file.
        unless (-e $path) {
            my $parent_filename = fileparse($path);
            $path = File::Spec->catfile($basedir, $parent_filename);
        }

        # We need the header
        my $hdr = read_header($path);

        # We need to know if there is a product. There are two ways of doing this.
        # 1. Look at the filename.
        # 2. Read the PRODUCT header
        # Reconstructing the filename will be tricky without a header but see how
        # far we can get
        $path =~ s/_0001_raw001/_raw001/;
        my $product;
        if (defined $hdr) {
            $product = $hdr->value("PRODUCT");
        }
        else {
            my @parts = dissect_drfile( $path );
            # raw is special so is not a real product
            if (defined $parts[5] && $parts[5] ne 'raw') {
                $product = $parts[5];
            }
        }

        # if there is no product it is a raw file so will not be
        # a FITS CADC filename.
        my $newpath;
        if (!$product || !$haspar) {
            # do nothing if this looks raw
            if (! looks_like_rawfile($path)) {
                # Recreate the raw file name
                if (defined $hdr) {
                    $newpath = construct_rawfile($hdr);
                }
                else {
                    # This hack does not work for multi-subsystem hybrids
                    # since we lose the subscan number
                    $newpath = $path;
                    if ($newpath =~ /_[a-z]+(\d+)/) {
                        my $newnum = sprintf( "%04d", $1);
                        $newpath =~ s/_[a-z]+\d+/_$newnum/;
                    }
                }
            }
        }
        else {
            # We need the ASN_TYPE from this parent to correctly make the file name
            my $asntype = (defined $hdr ? $hdr->value("ASN_TYPE") : undef);

            # if we do not have a value we need to hope it's an
            # "obs" product
            $newpath = drfilename_to_cadc(
                $path,
                (defined $asntype ?  (ASN_TYPE => $asntype) : ()),
                ((defined $asntype and $asntype eq 'public')
                    ? (VERSION => $version) : ()));
        }

        return $newpath;
    }
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
