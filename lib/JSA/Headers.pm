package JSA::Headers;

=head1 NAME

JSA::Headers - Helper functions to deal with file headers.

=head1 SYNOPSIS

    use JSA::Headers;
    update_fits_product($fits_file);

=head1 DESCRIPTION

This module provides helper functions that handle file headers for
both NDFs and FITS files.

=cut

use strict;
use warnings;
use warnings::register;

use Scalar::Util qw/looks_like_number/;
use Astro::FITS::CFITSIO;
use Astro::FITS::HdrTrans;
use Astro::FITS::Header::NDF;
use Astro::FITS::Header::CFITSIO;
use Carp;
use Image::ExifTool qw/:Public/;
use NDF 1.47;
use Starlink::Config qw/:override/;

use JSA::Files qw/drfilename_to_cadc/;

use Exporter 'import';
our @EXPORT_OK = qw/read_headers read_header read_wcs get_header_value
                    get_orac_instrument update_fits_product
                    cadc_ack read_jcmtstate/;

=head1 FUNCTIONS

=over 4

=item B<get_header_value>

Retrieve number of values matching a given FITS header keyword.

    my ($number, %values) = get_header_value($key, @headers);

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

    foreach my $h (@hdrs) {
        my $value = $h->value($key);

        if (defined $value) {
            $values{$value} ++;
            $nhits ++;
        }
    }

    return ($nhits, %values);
}

=item B<get_orac_instrument>

Determine the instrument for a given set of headers. This instrument
can then be used to initialize ORAC-DR.

    my $instrument = get_orac_instrument($header);

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
    unless (UNIVERSAL::isa( $hdr, "Astro::FITS::Header" )) {
        croak "Input to get_orac_instrument must be an Astro::FITS::Header object";
    }

    # We need the INSTRUMENT and the BACKEND
    my %fits;
    tie %fits, "Astro::FITS::Header", $hdr;
    my $class = Astro::FITS::HdrTrans::determine_class(\%fits, undef, 1);

    unless (defined $class) {
      croak "Unable to determine header translation class";
    }

    my $instrument = $class->to_INSTRUMENT(\%fits);
    my $backend = $class->can('to_BACKEND') ? $class->to_BACKEND(\%fits)
                                            : undef;

    my $oa;
    unless (defined $backend) {
        # Without knowing the backend, must just use the instrument name.
        $oa = $instrument;
    }
    elsif ($backend eq 'ACSIS' || $backend eq 'DAS' || $backend eq 'AOSC') {
        $oa = "ACSIS";
    }
    elsif ($instrument eq 'SCUBA-2') {
        $oa = 'SCUBA2';

        my $subsysnr = $fits{'SUBSYSNR'};

        if (defined($subsysnr)) {
            $oa .= "_$subsysnr";
        }

    }
    else {
        # go with instrument
        $oa = $instrument;
    }

    return $oa;
}

=item B<read_headers>

Read headers from a list of files.

    my %headers = read_headers(@files);

Returns a hash, keys being the filename and values being an
Astro::FITS::Header object created from reading the header for the
given filename.

=cut

sub read_headers {
    my @files = @_;

    my %headers;

    foreach my $f (@files) {
        my $hdr = read_header($f);
        $headers{$f} = $hdr if defined $hdr;
    }

    return %headers;
}

=item B<read_header>

Read header as Astro::FITS::Header object from a single file.

    $hdr = read_header($file);

Can be FITS or NDF.

=cut

sub read_header {
    my $f = shift;
    my $hdr;

    if ($f =~ /\.f.*$/i) {
        $hdr = eval {Astro::FITS::Header::CFITSIO->new(File => $f)};
    }
    elsif ($f =~ /\.sdf$/) {
        $hdr = eval {Astro::FITS::Header::NDF->new(File => $f)};
    }
    elsif ($f =~ /\.png$/) {
        $hdr = eval {ImageInfo($f)};
    }

    return $hdr;
}

=item B<read_wcs>

Reads the AST Frameset from a data file. If the file is an NDF the AST
frameset is read directly, otherwise the header is read and WCS extracted.

    $wcs = read_wcs($file);

=cut

sub read_wcs {
    my $f = shift;

    my $wcs;

    if ($f =~ /\.sdf$/) {
        my $status = &NDF::SAI__OK();
        err_begin($status);
        ndf_begin();

        # Retrieve the WCS from the NDF.
        ndf_find(&NDF::DAT__ROOT(), $f, my $indf, $status);
        $wcs = ndfGtwcs($indf, $status);
        ndf_annul($indf, $status);
        my $errstr;
        if ($status != &NDF::SAI__OK()) {
          $errstr = &NDF::err_flush_to_string($status);
        }
        ndf_end($status);
        err_end($status);
        throw JSA::Error::FatalError("Error reading WCS from file $f: $errstr")
            if defined $errstr;

    }
    else {
        my $header = read_header($f);
        $wcs = $header->get_wcs();
    }

    return $wcs;
}

=item B<read_jcmtstate>

Read the JCMTSTATE information from a raw data file.

    %state = read_jcmtstate($file, 'start', @items);

Where the second argument can be 'start' or 'end' to indicate
whether the state values are read first or last entry in
the file. Additionally, a positive integer can be used to
specify a particular position in the sequence. The latter
usage requires that the number of entries in the file
is known. It can also be a reference to an array indicating
multiple indices. Finally, an undefined
value can indicate all entries should be retrieved.

By default all items will be read and returned in a hash
indexed by component name. If a subset is required then
the list can be supplied as additional arguments.

If multiple positions are to be read, results will be returned
as a reference to an array indexed by component name. If only
one position is requested a simple scalar will be used. Note that
if there is only one time slice but all were requested then that
single time slice will be returned using array references to avoid
surprises to the caller.

RTS_TASKS is never read by default. It can be read if explicitly
specified.

=cut

sub read_jcmtstate {
    my ($file, $upos, @items) = @_;

    # Open up the file, retrieve the JCMTSTATE structure, and store it
    # in our cache.
    my $status = &NDF::SAI__OK();
    err_begin($status);

    hds_open($file, "READ", my $loc, $status);
    dat_find($loc, "MORE", my $mloc, $status);
    dat_find($mloc, "JCMTSTATE", my $jloc, $status);
    dat_annul($mloc, $status);

    # find out how many time slice there are going to be
    # Assumes that RTS_NUM is first so won't be compressed.
    dat_index($jloc, 1, my $iloc, $status);
    dat_size($iloc, my $size, $status);
    dat_annul($iloc, $status);

    # Error string indicating that we had a problem and should clean up
    my $errstr;

    # work out which position to use. Default to first index
    my @posns;
    if (defined $upos && $status == &NDF::SAI__OK()) {
        my @inpos = ( ref($upos) ? @$upos : ($upos) );

        # Loop over all input positions
        foreach my $p (@inpos) {
            $p = lc($p);
            my $isnum = looks_like_number($p);

            # do not need to check if we are only asking for first slice
            if ($p eq 'start') {
                push(@posns, 1);
            }
            elsif ($isnum && $p == 1) {
                push(@posns, 1);
            }
            elsif ($isnum || $p eq 'end' ) {
                if ($p eq 'end') {
                    push(@posns, $size );
                }
                elsif ($p > 0 && $p <= $size) {
                    push(@posns, $p);
                }
                else {
                    $errstr = "Requested data from JCMTSTATE slice $p but this is out of range 1 <= $p <= $size";
                }
            }
            else {
                $errstr = "Error reading JCMTSTATEfrom file $file, position '$p' not recognized";
            }
        }
    }

    # Decide whether we are accessing cells or the full array
    # Use cell if explicit items have been specified.
    my $use_cell = (@posns ? 1 : 0);

    # Get a hash indicating which items are requested
    my %items = map {uc($_), undef} @items;

    # find out how many extensions we have
    dat_ncomp($jloc, my $ncomp, $status);

    # Somewhere to store the results
    my %results;

    # Keep a count of how many we retrieved
    my $found = 0;

    # Loop over each
    if ($status == &NDF::SAI__OK && !defined $errstr) {
        foreach my $i (1 .. $ncomp) {
            last if $status != &NDF::SAI__OK;

            dat_index($jloc, $i, my $iloc, $status);
            dat_name($iloc, my $name, $status);
            my @dims;
            dat_shape($iloc, 1, @dims, my $actdim, $status);

            # Check for special case of 1 element vector
            my $is_array = ($actdim == 0 ? 0 : 1);

            # skip if we are selecting a subset
            next if (@items && !exists $items{$name});

            # Skip RTS_TASKS unless we are asking for it
            next if (!@items && $name eq 'RTS_TASKS');

            $found ++;

            # Need the type to decide what to call next
            dat_type($iloc, my $type, $status);

            my $coderef;
            if ($type =~ /^_(DOUBLE|REAL)$/) {
                $coderef = ($use_cell ? \&dat_get0d : \&dat_getvd);
            }
            elsif ($type eq '_INTEGER') {
                $coderef = ($use_cell ? \&dat_get0i : \&dat_getvi);
            }
            else {
                $coderef = ($use_cell ? \&dat_get0c : \&dat_getvc);
            }

            my @values;
            if ($use_cell) {
                if (!$is_array) {
                    # this is actually a scalar so the value is a constant
                    $coderef->($iloc, my $val, $status);
                    @values = map {$val} (0 .. $#posns);
                }
                else {
                    foreach my $c (@posns) {
                        my @cell = ($c);
                        dat_cell($iloc, 1, @cell, my $cloc, $status);
                        $coderef->($cloc, my $val, $status);
                        dat_annul($cloc, $status);
                        push(@values, $val);
                    }
                }
            }
            else {
                $coderef->($iloc, $size, \@values, my $el, $status);

                if ($el < $size && $el == 1) {
                    # duplicate scalar items
                    my $val = $values[0];
                    @values = map {$val} (1 .. $size);
                }
            }

            # store the results (do not use a scalar if we asked for all entries)
            $results{$name} = ((@values > 1 || !$use_cell) ? \@values : $values[0]);

            # free the locator associated with this component
            dat_annul($iloc, $status);
        }
    }

    dat_annul($jloc, $status);
    dat_annul($loc, $status);

    if ($status != &NDF::SAI__OK()) {
        $errstr .= &NDF::err_flush_to_string($status);
    }

    err_end($status);
    throw JSA::Error::FatalError("Error reading JCMTSTATE from file $file: $errstr")
        if defined $errstr;

    # report if we did not find all that was requested
    if (@items && $found != @items) {
        throw JSA::Error::FatalError("Requested ".@items." components but only found $found");
    }

    return %results;
}

=item B<update_fits_product>

Update CADC-specific FITS headers in a FITS file.

    update_fits_product($file);

This function updates one FITS header:

 o PRODUCT

This function takes one argument: the FITS file to be updated.

This function does not return anything.

=cut

sub update_fits_product {
    my $file = shift;

    my $status = 0;
    my $ifits = Astro::FITS::CFITSIO::open_file(
        $file, Astro::FITS::CFITSIO::READWRITE(), $status);

    $ifits->get_num_hdus(my $numhdus, $status);

    # we only have to modify extensions
    if ($numhdus > 1) {
        # Read PRODUCT from PRIMARY header
        $ifits->read_key(Astro::FITS::CFITSIO::TSTRING(),
                         "PRODUCT", my $prodref, my $pcomment, $status);

        foreach my $i (2 .. $numhdus) {
            last if $status != 0;
            $ifits->movabs_hdu($i, my $hdutype, $status);
            next unless $hdutype == Astro::FITS::CFITSIO::IMAGE_HDU();

            # Get the EXTNAME
            $ifits->read_key(Astro::FITS::CFITSIO::TSTRING(),
                             "EXTNAME", my $extname, undef, $status);
            if ($status != 0) {
                $status = 0;
                next;
            }

            # Need thing after last dot
            $extname = (split(/\./,$extname))[-1];

            # set the new value for use PRODUCT (lower case version of extension)
            my $newprod = $prodref . lc("_$extname");

            $ifits->update_key(Astro::FITS::CFITSIO::TSTRING(),
                               "PRODUCT", $newprod, undef, $status);

            # Update the header checksum
            $ifits->update_chksum($status);

            $status = 0;
        }
    }

    $ifits->close_file($status);
}

=item B<cadc_ack>

Return the standard CADC acknowledgement text. Should be added as comment to output FITS
header.

    @text = cadc_ack();

=cut

{
    my @text = ();
    sub cadc_ack {
        unless (scalar @text) {
            @text = <DATA>;
            chomp(@text);
        }
        return @text;
    }
}

=back

=head1 AUTHORS

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>,
Brad Cavanagh E<lt>b.cavanagh@jach.hawaii.eduE<gt>,

=head1 COPYRIGHT

Copyright (C) 2008-2009 Science and Technology Facilities Council.
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

__DATA__

ACKNOWLEDGEMENTS:
If you have used CADC facilities and products (such as these data)
for your research, please include the following acknowledgement:

"This research used the facilities of the Canadian Astronomy Data
Centre operated by the National Research Council of Canada with
the support of the Canadian Space Agency."

The following acknowledgement should appear at some point in any
published papers containing data obtained with the JCMT:

"The James Clerk Maxwell Telescope is operated by the East Asian
Observatory on behalf of The National Astronomical Observatory of
Japan; Academia Sinica Institute of Astronomy and Astrophysics;
the Korea Astronomy and Space Science Institute; the Operation,
Maintenance and Upgrading Fund for Astronomical Telescopes and
Facility Instruments, budgeted from the Ministry of Finance
(MOF) of China and administrated by the Chinese Academy of
Sciences (CAS), as well as the National Key R&D Program of China
(No. 2017YFA0402700). Additional funding support is provided
by the Science and Technology Facilities Council of the United
Kingdom and participating universities in the United Kingdom and
Canada."

The following acknowledgement should appear at some point in any
published papers containing data obtained with the JCMT prior to
the handover to EAO on March 1 2015:

"The James Clerk Maxwell Telescope has historically been operated by
the Joint Astronomy Centre on behalf of the Science and Technology
Facilities Council of the United Kingdom, the Netherlands
Organisation for Scientific Research, and the National Research
Council of Canada."

Authors are also asked to give the identification number(s), i.e.
"Program ID", of the program(s) under which their data were obtained,
e.g. M07AU05, M07BC13, M07BN10, or M07AI24. We recommend that this
reference to the Program ID be made in the acknowledgement section
at the end of the paper or in the Observations section of the paper.
