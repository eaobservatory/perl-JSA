package JSA::Files;

=head1 NAME

JSA::Files - File naming and URIs in the JCMT Science Archive

=head1 SYNOPSIS

    use JSA::Files qw//;

=head1 DESCRIPTION

Helper routines for generating file names that are JSA compliant or
for converting URIs into filenames.

=cut

use strict;
use Carp;
use warnings;
use File::Spec;
use warnings::register;

use File::Copy qw/copy/;
use Image::ExifTool;
use File::SearchPath qw/searchpath/;
use Astro::FITS::HdrTrans qw/translate_from_FITS/;
use JSA::Error;

use Exporter 'import';
our @EXPORT_OK = qw/drfilename_to_cadc
                    dissect_drfile dissect_cadcfile
                    cadc_to_drfilename
                    looks_like_drfile looks_like_fits_drfile
                    looks_like_cadcfile
                    looks_like_rawfile cadc_transfer_check
                    compare_file_lists scan_dir construct_rawfile
                    can_send_to_cadc can_send_to_cadc_guess
                    looks_like_drthumb merge_pngs want_to_send_to_cadc/;

our $DEBUG = 0;

# List of support product types as found in ASN_TYPE FITS header
# along with corresponding names found in filenames.
my %PRODUCT_TYPES = (
        night => 'nit',
        public => 'pub',
        obs => 'obs',
        project => 'pro',
);

# invert it
my %FILE_ABBREV_TO_PROD_TYPE;
while (my ($k,$v) = each(%PRODUCT_TYPES)) {
    $FILE_ABBREV_TO_PROD_TYPE{$v} = $k;
}

# Products and associations to look for.
our @ASSOCS = qw/obs night project public/;
our @PRODUCTS = qw/healpix reduced rimg rsp/;
our %EXTRA_PRODUCTS = (
    public => [qw/extent-mask extent-cat extent-moc peak-cat tile-moc/],
);

# Set up a hash.
our %PRODS = map {$_ => {map {$_ => undef} @PRODUCTS}} @ASSOCS;

for my $assoc (keys %EXTRA_PRODUCTS) {
    for my $prod (@{$EXTRA_PRODUCTS{$assoc}}) {
        $PRODS{$assoc}{$prod} = undef;
    }
}

my $JCMTINFO = "/home/cadcops/bin/jcmtInfo";

=head1 FUNCTIONS

=over 4

=item B<can_send_to_cadc>

Determine whether or not an NDF can be converted to a FITS file for
ingest by CADC.

    $convert = can_send_to_cadc($mode, $header);

A file can be converted if it is a science observation and its product
type is listed in the association type array.

The only argument is an C<Astro::FITS::Header> item created from the
NDF.

=cut

sub can_send_to_cadc {
    my $mode = shift;
    my $header = shift;

    return 0 unless UNIVERSAL::isa($header, "Astro::FITS::Header");

    # if there is a SIMULATE header it should be False
    my $simitem = $header->itembyname("SIMULATE");
    return 0 if (defined $simitem && $simitem->value());

    my $inst = $header->value( "INSTRUME" );

    # For SCUBA there is no obs_type header but we simply want
    # to harvest all files with matching product
    if ($inst ne "SCUBA" and not ((defined $mode) and $mode eq 'public')) {
        my $obstype = $header->value("OBS_TYPE");

        return 0 unless defined $obstype;

        # For SCUBA-2 we can transfer pointing observations
        if ($inst =~ /SCUBA\-?2/i) {
            return 0 unless $obstype =~ /science|pointing/i;
        }
        else {
            return 0 unless $obstype =~ /science/i ;
        }
    }

    my $assoc = $header->value("ASN_TYPE");
    my $product = $header->value("PRODUCT");

    # This function is often called before the association type
    # header has been updated.  So if we read "night" then we
    # know we have a group product but that could be wrong.
    # If given an operating mode, use this in place of the association
    # type header for group products.
    $assoc = $mode if ((defined $mode) and ($assoc eq 'night'));

    return _can_send_to_cadc_quick( $assoc, $product );
}

=item B<want_to_send_to_cadc>

All we really know is whether we are interested in group
files or just "obs" files. If we are given a mode
string that is "obs" we only return true if the supplied
file header is an "obs". For others we return true.

    $want_file = want_to_send_to_cadc($mode, %opts);

Options hash indicates whether we are comparing to a
header hash, mode string, or filename, in that priority
order.

    header => Astro::FITS::Header object
    mode => mode from file
    filename => Name of file

=cut

sub want_to_send_to_cadc {
    my $mode = uc(shift);
    my %opts = @_;

    # Short circuit test
    return 1 if $mode ne "OBS";

    my $assoc;
    if (exists $opts{header}) {
        $assoc = uc($opts{header}->value("ASN_TYPE"));
    }
    elsif (exists $opts{mode} && defined $opts{mode} ) {
        $assoc = uc($opts{mode});
    }
    elsif (exists $opts{filename} && defined $opts{filename} ) {
        my @parts = dissect_drfile($opts{filename});
        $assoc = ($parts[0] ? "NIGHT" : "OBS" );
    }
    else {
      return 0;
    }

    if ($mode eq "OBS" &&  $assoc eq "OBS") {
      return 1;
    }

    return 0;
}

=item B<can_send_to_cadc_guess>

Determines whether a file should be sent to CADC solely based on the
filename.

    $can = can_send_to_cadc_guess($filename);

This is sometimes sufficient to determine whether something is suitable
without having to open up the file. This will usually be valid if the
PRODUCT header matches the product name embedded in the file. In some
cases you want to be sure that a file is okay to ignore if it is
missing but is listed in the provenance of another.

Group observations that look like DR files are always assumed to be
"night" products.

=cut

sub can_send_to_cadc_guess {
    my $file = shift;

    my ($product, $asntype);
    if (looks_like_drfile($file)) {
        my @parts = dissect_drfile( $file );
        $product = $parts[5];
        $asntype = ($parts[0] ? "night" : "obs" );
    }
    elsif (looks_like_cadcfile($file)) {
        # do not simply assume that a cadc file can be sent to cadc(!!!)
        my @parts = dissect_cadcfile($file);
        $product = $parts[4];
        $asntype = $FILE_ABBREV_TO_PROD_TYPE{$parts[6]};
    }
    elsif (looks_like_rawfile($file)) {
        return 1;
    }

    return _can_send_to_cadc_quick($asntype, $product);

}

=item B<merge_pngs>

Given a list of acceptable PNGs, merge them so that the rsp is on the
left and the rimg is on the right. The resulting PNGs will be named
_reduced_ in place of the _rimg_ and _rsp_ in the original file
names.

    $merged = merge_pngs( @inputs );

Returns a reference to an array of the merged PNG names. If no PNGs
were successfully merged for whatever reason, the returned array will
be empty.

Requires ImageMagick, specifically the 'montage' command, which will
be looked for in the path.

In addition to merging _rimg with _rsp, the original _rimg and _rsp
files are deleted. Furthermore any unattached _rimg files are
renamed to be "_reduced".

If any of the images has a "hpx" product component in the name
the output merged image will be "_healpix_" rather than "_reduced_".

=cut

sub merge_pngs {
    my @inputs = @_;

    # Product label to use for reduced data
    my $label = "reduced";

    # Split off the rimgs and the rsps, storing the rimgs in a hash for
    # ease of search.
    my %rimgs = map {$_ => undef} grep {/_(hpx)?rimg[-_]/} @inputs;
    my @rsps = grep {/_(hpx)?rsp[-_]/} @inputs;

    # This will contain list of inputs to be deleted at end
    # because we have handled them
    my @toremove;

    # Array to hold a list of merged PNGs.
    my @reduced;

    # Check for montage. We include "/usr/bin/" and "/usr/local/bin" explicitly
    my $montage = searchpath("montage", $ENV{PATH} . ":/usr/bin:/usr/local/bin");

    my $usemontage = (defined $montage && -e $montage);

    # Process all the _rsp images and find associated _rimg
    foreach my $rsp (@rsps) {
        my $usenull;

        # Get the size.
        my $size;
        if ($rsp =~ /_(\d{2,4})\.png$/) {
            $size = $1;
        }
        else {
            # Use the EXIF data -- this should never happen though
            my $exif = new Image::ExifTool;
            $exif->ExtractInfo($rsp);
            $size = $exif->GetValue("ImageHeight");
        }

        # Look for _hpxrimg_ before looking for a normal _rimg_
        (my $hpxrimg = $rsp) =~ s/_(?:hpx)?rsp([-_])/_hpxrimg$1/;
        (my $tanrimg = $rsp) =~ s/_(?:hpx)?rsp([-_])/_rimg$1/;

        my @tryrimg;
        push(@tryrimg, $hpxrimg, $tanrimg);

        # Have we got a rimg?
        my $rimg;
        for my $test (@tryrimg) {
            if (exists $rimgs{$test}) {
                $rimg = $test;
                last;
            }
        }

        # Determine which product label to use. Use healpix if either
        # the rsp or the rimg have a hpx prefix.
        my $prodlabel = $label;
        if ($rsp =~ /_hpx/ || (defined $rimg and $rimg =~ /_hpx/)) {
            $prodlabel = "healpix";
        }

        # Form the reduced name from this rsp.
        (my $reduced = $rsp) =~ s/_(?:hpx)?rsp([-_])/_${prodlabel}$1/;

        # Set up and run the command. At this point if there's an error
        # just don't do anything, but if it succeeds, push the name of
        # the resulting file onto our array for return later.
        if ($usemontage and defined $rimg) {
            my $command = "$montage $rsp $rimg -tile 2x1 -background lightgray -geometry ${size}x${size}+0+0 $reduced";
            my $returnval = system($command);

            unless ($returnval) {
                # We have dealt with the _rimg (if we had one)
                if (defined $rimg) {
                    push @toremove, $rimg;
                    delete $rimgs{$rimg};
                }

                # Montage does not copy any EXIF headers over to the output
                # so we have to do that manually
                my $exif = Image::ExifTool->new();
                $exif->ExtractInfo($rsp);
                my @keywords = $exif->GetValue('Keywords');

                # Now write out what we have, skipping the RA and Dec keys
                # since they will be wrong for a merged image
                my $outexif = Image::ExifTool->new();
                foreach my $k (@keywords) {
                    next if $k =~ /astro:(RA|Dec)/;
                    $outexif->SetNewValue(Keywords => $k);
                }

                $outexif->WriteInfo($reduced);

            }
            else {
              croak "Error running montage";
            }
        }
        else {
            # No montage or no _rimg so just copy the _rsp to reduced
            copy($rsp, $reduced);
        }

        # indicate that we have dealt with the _rsp
        push @toremove, $rsp;

        # We have written a merged file
        push @reduced, $reduced;
    }

    # Now for all the entries left in %rimgs we need to rename them to reduced
    for my $rimg (keys %rimgs) {
        # Form the appropriate rimg and reduced names from this rimg.
        my $prodlabel = ($rimg =~ /_hpxrimg/ ? "healpix" : $label);
        (my $reduced = $rimg) =~ s/_(?:hpx)?rimg([-_])/_${prodlabel}$1/;
        copy($rimg, $reduced);
        push(@toremove, $rimg);
        push(@reduced, $reduced);
    }

    # For all the files that we have renamed to _reduced we delete
    # them if they look like temporary CADC preview images. We should
    # probably use a standardised pattern match but we are assuming
    # that JSA::Convert::rename_png() has been called.
    for my $png (@toremove) {
        if ($png =~ /_preview_\d{2,4}\.png$/) {
            unlink $png;
        }
    }

    return \@reduced;
}

=item B<looks_like_rawfile>

Examines the supplied filename and determines whether it looks like
a raw data file.

    $israw = looks_like_rawfile($filename);

The file suffix must be ".sdf".

=cut

sub looks_like_rawfile {
    my $filename = shift;
    $filename = _strip_path($filename);

    if ($filename =~ /^[ah]\d{8}_\d{5}_\d\d_\d{4}\.sdf$/) {
        # ACSIS
        return 1;
    }
    elsif ($filename =~ /^s[48][abcd]\d{8}_\d{5}_\d{4}\.sdf$/) {
        # SCUBA-2
        return 1;
    }
    elsif ($filename =~ /^\d{8}_dem_\d{4}(_\d)?\.sdf$/) {
        # SCUBA
        return 1;
    }

    return 0;
}

=item B<looks_like_drfile>

Examines the supplied filename to determine whether it looks like
a data file produced by the DR pipeline.

    $isdr = looks_like_drfile($filename);

The file suffix must be ".sdf".

Directory information is stripped prior to the check.

=cut

sub looks_like_drfile {
    my $filename = shift;
    $filename = _strip_path($filename);

    # The pattern matches are not full proof if a UT date has the year
    # 3000 in it for example

    # do not check that the "a" corresponds to the correct UT date for
    # DAS -> ACSIS transition
    if ($filename =~ /^g?[ah]\d{8}_\d{1,5}_\d\d?_[a-z]+(\d{3,6})?\.sdf$/) {
        # ACSIS
        return 1;
    }
    elsif ($filename =~ /^g?s\d{8}_\d{1,5}_\d{3}_\w+\.sdf$/) {
        # SCUBA-2
        # Don't include files with a trailing plain number segment which can
        # appear in the provenance because Starlink applications create them,
        # because including them causes dissect_drfile to fail when the
        # provenance is being processed.  You can sometimes get away with it
        # if the Starlink application happened to write a 2--4 digit number
        # because then it looks like a thumbnail resolution as far as
        # dissect_drfile is concerned, but that's not helpful because we're
        # looking for SDF files rather than thumbnails.
        return 0 if $filename =~ /_\d+\.sdf$/;
        return 1;
    }
    elsif ($filename =~ /^gs[48]50um_[-a-z]+\d{6}\.sdf$/) {
        # SCUBA-2 co-added JSA tile, or product generated from it.
        return 1;
    }
    elsif ($filename =~ /^ga\d{6}MHz-(?:250|1000)MHz-[ULS]SB_[-a-z]+\d{6}\.sdf$/) {
        # ACSIS co-added JSA tile, or product generated from it.
        return 1;
    }
    elsif ($filename =~ /^\d{8}_\d{4}_(resw|flat)\.sdf$/ ||
           $filename =~ /^\d{1,8}_\d{4}_(sho|lon|p13|p20|p11)_\w+\.sdf$/ ||
           $filename =~ /^\d{8}_grp_\d{4}_\w+_(long|short|p2000|p1100|p1350)\.sdf$/) {
        # SCUBA
        return 1;
    }

    return 0;
}

=item B<looks_like_fits_drfile>

Determine whether a file looks like a FITS file produced by the
DR pipeline.

=cut

sub looks_like_fits_drfile {
    my $filename = _strip_path(shift);

    if ($filename =~ /^gs[48]50um_[-a-z]+\d{6}\.fits$/) {
        # Product from SCUBA-2 co-added JSA tile.
        return 1;
    }

    return 0;
}

sub looks_like_drthumb {
    my $filename = shift;
    $filename = _strip_path($filename);

    if ($filename =~ /\.png$/) {
        return 1;
    }

    return 0;
}

=item B<looks_like_cadcfile>

See if the supplied file looks like it uses the CADC naming convention.

    $iscadc = looks_like_cadcfile($file);

The file suffix must be ".fits". Directory information is stripped
before testing the file for compliance.

=cut

sub looks_like_cadcfile {
  my $filename = shift;
  $filename = _strip_path($filename);

  # These pattern matches are not bulletproof
  if ($filename =~ /^jcmth\d{8}_\d{5}_\d{2}_\w+_[a-z]{3}_\d{3}(_\d{2,4})?\.(fits|png)/) {
      return 1; # Heterodyne
  }
  elsif ($filename =~ /^jcmts\d{8}_\d{5}_\d{3}_\w+_[a-z]{3}_\d{3}(_\d{2,4})?\.(fits|png)/) {
      return 1; # SCUBA-2
  }
  elsif ($filename =~ /^jcmts\d{8}_\d{5}_(lon|sho|p20|p13|p11|mix)_\w+_[a-z]{3}_\d{3}(_\d{2,4})?\.(fits|png)/) {
      return 1; # SCUBA
  }
  elsif ($filename =~ /^jcmt_.*_preview_\d+\.png$/) {
      return 1; # CAOM-2 style preview filename: <collection>_<observationID>_<productID>_preview_(64|256|1024)
  }
  elsif ($filename =~/^jcmts[48]50um_[-a-z]+\d{6}_pub_\d{3}\.fits$/) {
      return 1; # SCUBA-2 co-added JSA tile
  }
  elsif ($filename =~/^jcmth\d{6}mhz-(?:250|1000)mhz-[uls]sb_[-a-z]+\d{6}_pub_\d{3}\.fits$/) {
      return 1; # ACSIS co-added JSA tile
  }

  print "Failed looks_like_cadcfile\n" if $DEBUG;
  return 0;
}


=item B<dissect_drfile>

Split a filename generated by the pipeline into its component parts.

    ($isgroup, $prefix, $utdate, $obsnum, $subsys, $product, $prodcount,
    $resolution, $suffix, $asn_id)
        = dissect_drfile( $drfile );

The group flag is a boolean. The observation number, subsystem number
(wavelength for SCUBA-2) and product count will not be zero-padded.
The product count can be undefined, as can the resolution.

Returns empty list if the supplied file does not look like a pipeline
product.

=cut

sub dissect_drfile {
    my $drfile = shift;
    my $original = $drfile;
    return () unless (looks_like_drfile($drfile) ||
                      looks_like_fits_drfile($drfile) ||
                      looks_like_drthumb($drfile));
    $drfile = _strip_path($drfile);

    my $isgroup = ($drfile =~ s/^g//);
    my ($prefix, $utdate, $obsnum, $subsys, $product, $prodcount,
        $resolution, $suffix);
    my $asn_id = undef;

    if ($drfile =~ /^(g?[ahs])(\d{8})_(\d{1,5})_(\d{1,3})_([a-z]+)(\d*)(?:_(\d{2,4}))?\.(sdf|png)$/) {
        $prefix = $1;
        $utdate = $2;
        $obsnum = $3;
        $subsys = $4;
        $product= $5;
        $prodcount = $6;
        $prodcount = undef if (defined $prodcount && $prodcount eq '');
        $resolution = $7;
        $suffix = $8;
    }
    elsif ($drfile =~ /^(\d{1,8})_(\d{4})_(resw|flat)\.sdf$/) {
        # SCUBA reduce switch or flatfield
        $prefix = "s";
        $utdate = $1;
        $obsnum = $2;
        $subsys = "mix";
        $product = $3;
        $isgroup = 0;
        $suffix = 'sdf';
    }
    elsif ($drfile =~ /^(\d{1,8})_(\d{4})_(sho|lon|p13|p20|p11)_(\w+)\.sdf$/) {
        # SCUBA obs products after sub instrument split
        $prefix = "s";
        $utdate = $1;
        $obsnum = $2;
        $subsys = $3;
        $product = $4;
        $isgroup = 0;
        $suffix = 'sdf';
    }
    elsif ($drfile =~ /^(\d{8})_grp_(\d{4})_(\w+)_(long|short|p2000|p1100|p1350)\.sdf$/) {
        # SCUBA group
        $prefix = "s";
        $utdate = $1;
        $obsnum = $2;
        $subsys = substr($4,0,3);
        $product = $3;
        $isgroup = 1;
        $suffix = 'sdf';
    }
    elsif ($drfile =~ /^(s)([48]50um)_([-a-z]+)(\d{6})(?:_(\d{2,4}))?\.(sdf|png|fits)$/) {
        # SCUBA-2 co-added JSA tile
        $prefix = $1;
        $asn_id = $2;
        $utdate = undef;
        $obsnum = undef;
        $subsys = undef;
        $product = $3;
        $prodcount = $4;
        $resolution = $5;
        $suffix = $6;
    }
    elsif ($drfile =~ /^(a)(\d{6}MHz-(?:250|1000)MHz-[ULS]SB)_([-a-z]+)(\d{6})(?:_(\d{2,4}))?\.(sdf|png|fits)$/) {
        # ACSIS co-added JSA tile
        $prefix = $1;
        $asn_id = $2;
        $utdate = undef;
        $obsnum = undef;
        $subsys = undef;
        $product = $3;
        $prodcount = $4;
        $resolution = $5;
        $suffix = $6;
    }
    else {
        JSA::Error::FatalError->throw("DR file '$original' looked okay but failed pattern match");
    }

    # Clean up strings
    $obsnum =~ s/^0+//;
    $subsys =~ s/^0+//;
    $prodcount =~ s/^0+// if defined $prodcount;

    return ($isgroup, $prefix, $utdate, $obsnum, $subsys, $product, $prodcount,
            $resolution, $suffix, $asn_id);
}

=item B<dissect_cadcfile>

Given a CADC filename, split it into its component parts.

    @parts = dissect_cadcfile( $cadcfile );

Where the parts are defined as

    prefix
    utdate
    obsnum
    subsys
    product
    prodcount
    type
    version
    asn_id

The .fits suffix is not included in the returned list but should be present
in the supplied filename.

=cut

sub dissect_cadcfile {
    my $cadcfile = shift;
    return () unless looks_like_cadcfile($cadcfile);
    $cadcfile = _strip_path($cadcfile);

    my ($prefix, $utdate, $obsnum, $subsys, $product, $prodcount,
        $type, $version, $asn_id);

    if ($cadcfile =~ /^jcmt([hs])(\d{8})_(\d{5})_(\d{2,3}|lon|sho|p20|p13|p11|mix)_(\w+)_([a-z]{3})_(\d{3})\.fits/) {
        $prefix = $1;
        $utdate = $2;
        $obsnum = $3;
        $subsys = $4;
        $asn_id = undef;
        $product= $5;
        $type = $6;
        $version = $7;

        # Split product into product and number.  Handle healpix specially
        # because the "normal" pattern just grabs the first 3 digits and
        # drops the second half of the JSA tile number.
        if ($product =~ /(healpix)(\d{6})/) {
            $product = $1;
            $prodcount = $2;
        }
        elsif ($product =~ /([a-z]+)(\d{3})/) {
            $product = $1;
            $prodcount = $2;
        }
    }
    elsif ($cadcfile =~ /^jcmt(s)([48]50um)_([a-z]+)(\d{6})_(pub)_(\d{3})\.fits$/) {
        $prefix = $1;
        $utdate = undef;
        $obsnum = undef;
        $subsys = undef;
        $asn_id = $2;
        $product = $3;
        $prodcount = $4;
        $type = $5;
        $version = $6;
    }
    elsif ($cadcfile =~ /^jcmt(h)(\d{6}mhz-(?:250|1000)mhz-[uls]sb)_([a-z]+)(\d{6})_(pub)_(\d{3})\.fits$/) {
        $prefix = $1;
        $utdate = undef;
        $obsnum = undef;
        $subsys = undef;
        $asn_id = $2;
        $product = $3;
        $prodcount = $4;
        $type = $5;
        $version = $6;
    }
    else {
        JSA::Error::FatalError->throw("CADC file '$cadcfile' looked okay but failed pattern match");
    }

    # Clean up strings
    $obsnum =~ s/^0+//;
    $subsys =~ s/^0+//;
    $prodcount =~ s/^0+// if defined $prodcount;
    $version =~ s/^0+//;

    return ($prefix, $utdate, $obsnum, $subsys, $product, $prodcount,
            $type, $version, $asn_id);
}

=item B<drfilename_to_cadc>

Convert a pipeline output filename into standard CADC file naming
convention.

    $cadcname = drfilename_to_cadc($drname);

Association type and version number can be supplied using hash syntax.
Version number will default to 0 and in almost all cases that is the
correct value. Association type is mandatory for all group files.

    $cadcname = drfilename_to_cadc($drname, ASN_TYPE => $type,
                                            VERSION => 0);

The type describes the association to be used for this file. Options
are 'obs', 'night', 'project', 'public' or the abbreviated translated
forms. The type is optional for an 'obs' file since the type can be determined
from the filename.

If the input file does not look like it is in the correct format,
an undefined value is returned.

The name conversion is attempted even if the supplied name does not
look like a DR product. The file is not opened to check that there
are PRODUCT and ASN_TYPE headers.

If a directory path is included in the supplied name it will
be included in the returned name.

=cut

sub drfilename_to_cadc {
    my $drname = shift;
    my %defaults = (ASN_TYPE => undef, VERSION => undef);
    my %args = (%defaults, @_);

    # Get the directory name
    my ($dir, $filepart) = _strip_path($drname);

    # Split file into components. If this returns empty then we know
    # it did not look like a drfile so no need to call looks_like_drfile
    my ($isgroup, $prefix, $utdate, $obsnum, $subsys, $product, $prodcount,
        $resolution, $suffix, $asn_id)
        = dissect_drfile($drname);

    if (! defined($args{'ASN_TYPE'}) && looks_like_drthumb($drname)) {
        if ($isgroup) {
            $args{'ASN_TYPE'} = 'night';
        }
        else {
            $args{'ASN_TYPE'} = 'obs';
        }
    }

    if (! defined($args{'VERSION'})) {
        $args{'VERSION'} = 0;
    }

    return () unless (defined $utdate) or (defined $asn_id);

    my $type = $args{ASN_TYPE};
    if ($isgroup) {
        # we will need a type
        if (!defined $type) {
            JSA::Error::BadArgs->throw("Must supply a association type for group products ($drname)");
        }
        elsif (exists $PRODUCT_TYPES{$type}) {
            $type = $PRODUCT_TYPES{$type};
        }
        elsif (exists $FILE_ABBREV_TO_PROD_TYPE{$type}) {
            # type is okay
        }
        else {
            JSA::Error::BadArgs->throw("drfilename_to_cadc: Unrecognized association type '$type' given");
        }

        # Should not get a type of "obs"
        if ($type eq 'obs') {
            JSA::Error::BadArgs->throw("This is a group observation but it is tagged as an 'obs' product");
        }

    }
    else {
        # Always force to 'obs' but warn if it was something different
        if (defined $type && $type ne 'obs') {
            warnings::warnif("File '$drname' looks like an 'obs' product but is tagged with '$type'. Forcing to 'obs'.");
        }

        $type = 'obs';
    }

    # Prefix of "a" is now meant to be "h"
    $prefix = "h" if $prefix eq 'a';

    # _reduced has a mandatory count and
    # scuba-2 does not yet include the count in the pipeline.
    if ($suffix ne 'png' && $product =~ /^reduced/ && ! defined $prodcount) {
        $prodcount = 1;
    }

    # The product count is only formatted if defined.  We need 6 digits if it
    # is a JSA tile file.  Assume that "healpix" products and everything
    # of "public" association type is a JSA tile.
    my $jsa_tiles = $product eq 'healpix'
                  || $type eq 'pub';
    my $p = (defined $prodcount ? $jsa_tiles ? '%06d' : "%03d" : "");

    # Note that we format subsystem as %02d because this will work
    # for SCUBA-2 850/450 without breaking ACSIS 2digit.
    my $subsys_format = '%02d';

    # SCUBA currently uses a string for subsys.  Detect this via the presence
    # of non-numbers rather than the absense of numbers because dissect_drfile
    # strips leading zeroes, changing subsystem '00' to '' (e.g. for ACSIS).
    if ($subsys =~ /[^0-9]/) {
        $subsys_format = '%s';
    }

    my $res_format = (defined( $resolution ) ? '_%d' : "");

    # Now form the new filename
    my $new;
    unless (defined $asn_id) {
        # Most JSA files have a filename based on UT date, obsnum etc.
        $new = sprintf('%s%s%08d_%05d_'.$subsys_format.'_%s'.$p.'_%s_%03d'.$res_format.'.%s',
                       "jcmt", $prefix, $utdate, $obsnum, $subsys,
                       $product, (defined $prodcount ? $prodcount : ()),
                       $type, $args{VERSION}, (defined $resolution ? $resolution : ()),
                       ($suffix eq 'png' ? 'png' : 'fits'));
    }
    else {
        # Co-added JSA tiled products have a filename based on ASN ID.
        $new = sprintf('%s%s%s_%s'.$p.'_%s_%03d'.$res_format.'.%s',
                       'jcmt', $prefix, $asn_id,
                       $product, (defined $prodcount ? $prodcount : ()),
                       $type, $args{'VERSION'}, (defined $resolution ? $resolution : ()),
                       ($suffix eq 'png' ? 'png' : 'fits'));
    }

    # CADC requested during the teleconference of 2014/05/13 that all file names
    # be lower case.
    $new = lc($new);

    # prepend directory if needed
    if ($dir) {
        $new = File::Spec->catfile($dir, $new);
    }

    return $new;
}

=item B<cadc_to_drfilename>

Convert a CADC formatted filename to the original DR filename.

    $drname = cadc_to_drfilename($cadcname);

Note that the 'h'->'a' replacement in prefix is dependent on
the content of the YYYYMMMDD ut date or the presence of an
association ID filename part.

Undef is returned if the file does not look like a CADC filename .

If a directory path is included in the supplied name it will
be included in the returned name.

=cut

sub cadc_to_drfilename {
    my $cadcfile = shift;

    # Get the directory name
    my ($dir, $filepart) = _strip_path($cadcfile);

    # split into parts. Will fail if name does not look like CADC name
    my @parts = dissect_cadcfile($cadcfile);
    return () unless @parts;
    return unless looks_like_cadcfile($cadcfile);

    # Sort out acsis prefix
    if ($parts[0] eq 'h' && ($parts[1] > 20060801 || defined $parts[8])) {
        $parts[0] = 'a';
    }

    # Is this an obs product or a group?
    my $isobs = ($parts[6] eq 'obs' ? 1 : 0);

    # see if this is plausibly a SCUBA observation
    my $is_scuba;
    if ($parts[0] eq 's' && $parts[3] =~ /^(lon|sho|p11|p13|p20|mix)/) {
        $is_scuba = 1;
    }

    my $new;
    if ($is_scuba) {
        if ($isobs) {
            if ($parts[4] =~ /(resw|flat)/) {
                $new = sprintf('%08d_%04d_%s'.".sdf", @parts[1, 2, 4]);
            }
            else {
                $new = sprintf('%08d_%04d_%s_%s'.".sdf", @parts[1, 2, 3, 4]);
            }

        }
        else {
            my %lut = (lon => "long", "sho" => "short", "p11" => "p1100",
                       "p13" => "p1350", "p20" => "p2000");
            JSA::Error::FatalError->throw("Do not know how to convert subsystem '$parts[4]' for group")
                unless exists $lut{$parts[3]};
            my $newtype = $lut{$parts[3]};
            $new = sprintf('%08d_grp_%04d_%s_%s'.".sdf", @parts[1, 2, 4], $newtype);
        }

    }
    else {
        # product formatting
        my $p = (defined $parts[5] ? $parts[4] eq 'healpix' ? '%06d' : "%03d" : "");

        unless (defined $parts[8]) {
            # zero padding depends on whether we are an obs or not
            my $obsfmt = '%05d';
            my $ssysfmt = '%02d';

            if (!$isobs) {
                $obsfmt = '%d';
                $ssysfmt = '%d';
            }

            $new = sprintf('%s%08d_'.$obsfmt.'_'.$ssysfmt.'_%s'.$p.'.sdf',
                           @parts[0 .. 4], (defined $parts[5] ? $parts[5] : ()));
        }
        else {
             $new = sprintf('%s%s_%s'.$p.'.sdf',
                            @parts[0, 8, 4], (defined $parts[5] ? $parts[5] : ()));
        }

        # account for group
        $new = "g".$new unless $isobs;
    }

    # prepend directory if needed
    if ($dir) {
        $new = File::Spec->catfile( $dir, $new );
    }
    return $new;
}

=item B<construct_rawfile>

Construct a raw filename given a FITS header object or a FITS hash.
Subsystem will be a number for ACSIS and the SUBARRAY for SCUBA-2.

   $raw = construct_rawfile(%hdr);

=cut

sub construct_rawfile {
    my %hdr;

    if (@_ == 1) {
        my $h = shift;
        tie %hdr, "Astro::FITS::Header", $h;
    }
    else {
        %hdr = @_;
    }

    # Need header translation to handle SCUBA vs more modern instrumentation
    my %trans = translate_from_FITS(\%hdr);

    my $ut   = $trans{UTDATE};
    my $inst = $trans{INSTRUMENT};
    my $be   = $trans{BACKEND};
    my $nsub = $hdr{NSUBSCAN};
    my $obs  = $trans{OBSERVATION_NUMBER};

    my $file;
    if (defined $be && $be =~ /(DAS|ACSIS)/) {
        my $prefix;

        if ($be =~ /DAS/) {
            $prefix = "h";
        }
        elsif ($be =~ /ACSIS/) {
            $prefix = "a";
        }
        else {
            throw JSA::Error::FatalError->new("Unrecognized backend '$be'");
        }

        $file = sprintf($prefix.'%08d_%05d_%02d_%04d.sdf', $ut, $obs, $hdr{SUBSYSNR}, $nsub);

    }
    elsif ($inst =~ /SCUBA\-?2/) {
        $file = $hdr{SUBARRAY}. sprintf('%08d_%05d_%04d.sdf', $ut, $obs, $nsub );
    }
    elsif ($inst eq "SCUBA") {
        # Note that this will not work for those special cased files with the _N suffix
        $file = sprintf('%08d_dem_%04d.sdf', $ut, $hdr{RUN});
    }
    else {
        throw JSA::Error::FatalError->new("Unrecognized instrument name $inst ".(defined $be ? " / $be " : ""));
    }

    return $file;
}

=item B<scan_dir>

Scan the current directory looking for files matching a particular
pattern. It runs stat() files or lstat() on links and return the
output of stat as a reference to an array in a hash indexed by
filename. The hash is suitably configured to be used by
compare_file_lists().

    %files = scan_dir(qr/\.(sdf|fits)$/);

If no pattern is supplied the default is to scan for FITS, SDF, and
PNG suffix filenames.

    %files = scan_dir();

=cut

sub scan_dir {
    my $pattern = shift;
    $pattern = qr/\.(sdf|fits|png)$/ unless defined $pattern;

    opendir(my $dh, File::Spec->curdir)
        or croak "Could not open data directory to scan it: $!";

    my %files;
    while (defined( my $file = readdir($dh))) {
        if ($file =~ $pattern) {
            if (-l $file) {
                $files{$file} = [lstat($file)];
            }
            else {
                $files{$file} = [stat($file)];
            }
        }
    }

    closedir($dh) or croak "Could not close data directory after scan: $!";
    return %files;
}

=item B<compare_file_lists>

Compare the scan done by C<scan_dir> before with the scan after and return anything that
is newer than before or is not present in the old scan.

    @new = compare_file_lists(\%old, \%new);

=cut

sub compare_file_lists {
    my $original = shift;
    my $after = shift;

    my @files;
    for my $current (keys %$after) {
        if (!exists $original->{$current}) {
            # must be a new file since it did not exist before
            push(@files, $current);
        }
        elsif ( $original->{$current}->[9] < $after->{$current}->[9]) {
            # modified since the original scan
            push(@files, $current);
        }
    }

    return @files;
}

=back

=begin PRIVATE

=head1 INTERNAL ROUTINES

=over 4

=item B<_strip_path>

Given a filename that may include a directory path, split the
name into the path and the base filename.

In scalar context returns just the base filename. In list context
returns the directory and file information.

    ($dir, $file) = _strip_path($path);
    $file  = _strip_path($path);

=cut

sub _strip_path {
    my $path = shift;

    my ($vol, $dir, $file) = File::Spec->splitpath($path);

    if (wantarray) {
        return ($dir, $file);
    }
    else {
        return $file;
    }
}

=item B<_can_send_to_cadc_quick>

Internal version of can_send_to_cadc* that simply tests
product name and association type for validity with CADC
rules.

    $can = _can_send_to_cadc_quick($assoc, $product);

=cut

sub _can_send_to_cadc_quick {
    my $assoc = shift;
    my $product = shift;
    return 0 if (! defined $assoc || ! defined $product);
    return 1 if exists $PRODS{$assoc}{$product};
    return 0;
}

=back

=end PRIVATE

=head1 AUTHORS

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>,

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
