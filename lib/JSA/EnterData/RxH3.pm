package JSA::EnterData::RxH3;

use strict;
use warnings;

use parent 'JSA::EnterData';

use Astro::FITS::CFITSIO qw/:longnames :constants/;
use Astro::FITS::Header;
use Astro::FITS::Header::Item;
use DateTime;
use DateTime::Duration;
use DateTime::Format::ISO8601;
use File::Spec;
use IO::Dir;
use List::MoreUtils qw/first_index/;
use OMP::DateTools;
use Starlink::AST;

=head1 NAME

JSA::EnterData::RxH3 - RxH3 specific enter-data methods.

=head1 METHODS

=over 4

=item new

Constructs a new C<EnterData> object.

=cut

sub new {
    my ($class, %args) = @_;

    my $obj = $class->SUPER::new(%args);
    return bless $obj, $class;
}

=item instrument_name

Returns the name of the instrument.

=cut

sub instrument_name {
    return 'RxH3';
}

=item instrument_table

Returns the name of the database table related to the instrument.

=cut

sub instrument_table {
    return 'RXH3';
}

=item preprocess_header

Copies entries from the first extension's header (since this is
where the data are for a FITS binary table) and then applies the
superclass method.

=cut

sub preprocess_header {
    my $self = shift;
    my $filename = shift;
    my $header = shift;
    my $extra = shift;

    # Copy all entries from the single subheader to the primary header,
    # overwriting those already there.
    my @subhdrs = $header->subhdrs();
    if (1 == scalar @subhdrs) {
        my $subhdr = $subhdrs[0];

        foreach my $item ($subhdr->allitems()) {
            my $keyword = $item->keyword();

            my $index = $header->index($keyword);
            if (defined $index) {
                $header->replace($index, $item);
            }
            else {
                $header->insert((scalar $header->allitems()), $item);
            }
        }
    }

    $self->SUPER::preprocess_header($filename, $header, $extra);
}

=item construct_missing_headers($filename, $header)

Make a set if header of additional information which should be included
in database records but may not be present in the raw data files themselves.

=cut

sub construct_missing_headers {
    my $self = shift;
    my $filename = shift;
    my $header = shift;
    my $extra = shift;

    my @subhdrs = $header->subhdrs();
    if (1 != scalar @subhdrs) {
        die 'Unexpected number of subheaders';
    }
    my $extheader = $subhdrs[0];

    my $has_date_obs = 1;
    my $date_str = $header->value('DATE-OBS');
    unless (defined $date_str) {
        $date_str = $header->value('DATE');
        $has_date_obs = 0;
    }
    die 'Neither DATE-OBS nor DATE is defined' unless defined $date_str;

    my $date = DateTime::Format::ISO8601->parse_datetime($date_str);

    my $obsnum = $extra->value('OBSNUM');
    die 'OBSNUM is not defined' unless defined $obsnum;
    my $obsid = sprintf('rxh3_%05d_%s', $obsnum, $date->strftime('%Y%m%dT%H%M%S'));

    my $freq = $extheader->value('FREQBAND') // $extheader->value('FREQ1');
    die 'Neither FREQBAND nor FREQ1 is defined' unless defined $freq;
    my $obsidss = sprintf('%s_%d', $obsid, int($freq));

    my $semester = OMP::DateTools->determine_semester(
        date => $date, tel => 'JCMT');

    my $missing = new Astro::FITS::Header(Cards => [
        new Astro::FITS::Header::Item(
            Keyword => 'INSTRUME',
            Value => uc($self->instrument_name()),
            Type => 'STRING'),
        new Astro::FITS::Header::Item(
            Keyword => 'MSBID',
            Value => 'CAL',
            Type => 'STRING'),
        new Astro::FITS::Header::Item(
            Keyword => 'OBS_TYPE',
            Value => 'holography',
            Type => 'STRING'),
        new Astro::FITS::Header::Item(
            Keyword => 'OBSID',
            Value => $obsid,
            Type => 'STRING'),
        new Astro::FITS::Header::Item(
            Keyword => 'OBSIDSS',
            Value => $obsidss,
            Type => 'STRING'),
        new Astro::FITS::Header::Item(
            Keyword => 'ORIGIN',
            Value => ($date < DateTime->new(year => 2015, month => 3, day => 20)
                ? 'Joint Astronomy Centre, Hilo'
                : 'East Asian Observatory'),
            Type => 'STRING'),
        new Astro::FITS::Header::Item(
            Keyword => 'PROJECT',
            Value => 'M' . $semester . 'EC09',
            Type => 'STRING'),
        new Astro::FITS::Header::Item(
            Keyword => 'TELESCOP',
            Value => 'JCMT',
            Type => 'STRING'),
        new Astro::FITS::Header::Item(
            Keyword => 'UTDATE',
            Value => 0 + $date->ymd(''),
            Type => 'INT'),
        new Astro::FITS::Header::Item(
            Keyword => 'NSUBSCAN',
            Value => 1,
            Type => 'INT'),
        new Astro::FITS::Header::Item(
            Keyword => 'STATUS',
            Value => 1 ? 'NORMAL' : 'ABORTED',  # TODO: determine how to set this
            Type => 'STRING'),
    ]);

    unless ($has_date_obs) {
        $missing->insert(0, new Astro::FITS::Header::Item(
            Keyword => 'DATE-OBS',
            Value => $date_str,
            Type => 'STRING'));
    }

    return $missing;
}

=item read_file_extra($filename)

Read extra information from a file.

=cut

sub read_file_extra {
    my $self = shift;
    my $filename = shift;

    # Extract end date from the file.
    my ($date_obs, $date_end) = $self->_get_date_range($filename);

    # See which number file this is for the night.
    my $index = $self->_get_file_index_number($filename);

    return new Astro::FITS::Header(Cards => [
        new Astro::FITS::Header::Item(
            Keyword => 'DATE-END',
            Value => $date_end->iso8601(),
            Type => 'STRING'),
        new Astro::FITS::Header::Item(
            Keyword => 'OBSNUM',
            Value => 1 + $index,
            Type => 'INT'),

    ]);
}

=item _get_date_range($filename)

Extract a datetime range from a RxH3 raw data file.

This examines the "TIME" column in the table and returns
the first and last values, converted to DateTime objects.

=cut

sub _get_date_range {
    my $self = shift;
    my $filename = shift;

    my $status = 0;

    my $fptr = Astro::FITS::CFITSIO::open_file(
        $filename, READONLY, $status);
    _check_fits_status($status);

    $fptr->get_num_hdus(my $num_hdus, $status);
    _check_fits_status($status);
    die 'Unexpected number of HDUs' unless $num_hdus == 2;

    $fptr->movabs_hdu(2, my $hdu_type, $status);
    _check_fits_status($status);
    die 'First extension not a binary table' unless $hdu_type == BINARY_TBL;

    $fptr->get_num_rows(my $num_rows, $status);
    _check_fits_status($status);

    $fptr->get_colnum(CASEINSEN, 'TIME', my $column, $status);
    _check_fits_status($status);

    $fptr->read_col(TFLOAT, $column, 1, 1, $num_rows, undef, my $time, undef, $status);
    _check_fits_status($status);

    $fptr->close_file($status);
    _check_fits_status($status);

    return (_mjd_to_datetime($time->[0]), _mjd_to_datetime($time->[-1]));
}

sub _check_fits_status {
    my $status = shift;

    return if $status == 0;

    Astro::FITS::CFITSIO::fits_get_errstatus($status, my $text);
    die 'Error reading FITS file: ' . $text;
}

sub _mjd_to_datetime {
    my $mjd = shift;

    die 'MJD value is null' unless defined $mjd;

    # This logic is based on that in SMURF's jcmtstate2cat.
    my $frame = new Starlink::AST::TimeFrame('TimeScale=TAI');
    $frame->SetD('TimeOrigin', $mjd);
    $frame->SetC('TimeScale', 'UTC');
    my $utc = $frame->GetD('TimeOrigin');

    return DateTime->from_epoch(epoch => 86400 * ($utc - 40587));
}

sub _get_file_index_number {
    my $self = shift;
    my $filename = shift;

    my (undef, $dir, $basename) = File::Spec->splitpath($filename);

    my $ut = $self->_filename_ut_date($basename);
    my $ut_str = $ut->ymd('');

    # Some RxH3 files are stored in the directory of the HST at the start
    # of the night, others in the directory of the UT date, so check both,
    # but only include files which match the UT date of interest.
    my @files = ();
    foreach my $date ($ut, $ut - new DateTime::Duration(days => 1)) {
        my $dir = new IO::Dir(File::Spec->catdir(
            $dir, File::Spec->updir(), $date->ymd('')));
        next unless defined $dir;
        while (defined (my $entry = $dir->read())) {
            next unless $entry =~ /rxh3-.*\.fits$/;
            my $entry_ut = $self->_filename_ut_date($entry)->ymd('');
            push @files, $entry if $entry_ut eq $ut_str;
        }
    }

    my $index = first_index {$_ eq $basename} sort @files;
    die "Could not find file '$basename' in its expected raw directories"
        if $index < 0;

    return $index;
}

sub _filename_ut_date {
    my $self = shift;
    my $filename = shift;

    # Parse the filename to get the approximate date and time.  Provided
    # holography is not done close to 2pm HST, this should be good enough
    # to determine the UT date.
    die 'Did not understand RxH3 filename'
        unless $filename =~ /rxh3-(\d{8})-(\d{6})\.fits$/;
    my $dt = DateTime::Format::ISO8601->parse_datetime($1 . 'T' . $2);
    $dt->set_time_zone('Pacific/Honolulu');
    $dt->set_time_zone('UTC');

    return $dt;
}

=item B<_do_verification>

Should we use JCMT::DataVerify?

=cut

sub _do_verification {
    my $self = shift;
    return 0;
}


=item _fill_headers_obsid_subsys

Fills obsid_subsysnr directly from OBSIDSS.

=cut

sub _fill_headers_obsid_subsys {
    my ($self, $header, $obsid) = @_;

    $header->{'obsid_subsysnr'} = $header->{'OBSIDSS'};
}

=item B<fill_headers_FILES>

Calls the superclass method and then extracts subsysnr
values from the last component of the OBSIDSS.

=cut

sub fill_headers_FILES {
    my $self = shift;
    my ($header, undef, undef) = @_;

    $self->SUPER::fill_headers_FILES(@_);

    if ($header->{'OBSIDSS'} =~ /_(\d+)$/) {
        $header->{'subsysnr'} = $1;
    }
}

1;

=back

Copyright (C) 2018 East Asian Observatory
All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 2 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful,but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,51 Franklin
Street, Fifth Floor, Boston, MA  02110-1301, USA

=cut
