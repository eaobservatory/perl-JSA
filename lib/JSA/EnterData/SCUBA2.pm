package JSA::EnterData::SCUBA2;

use strict;
use warnings;

use Carp qw/carp/;
use Data::Dumper;
use File::Spec;
use List::Util qw/first/;

use JSA::DB::TableCOMMON;

use parent 'JSA::EnterData';

=head1 NAME

JSA::EnterData::SCUBA2 - SCUBA2 specific methods.

=head1 SYNOPSIS

    # Create new object, with specific header dictionary.
    my $scuba2 = new JSA::EnterData::SCUBA2();

    my $name = $scuba2->instrument_name();

    my @cmd = $scuba2->get_bound_check_command;
    system(@cmd) == 0
        or die "Problem with running bound check command for $name.";

    # Use table in a SQL later.
    my $table = $scuba2->instrument_table();


=head1 DESCRIPTION

JAS::EnterData::SCUBA2 is a object oriented module, having instrument specific
methods.

=head2 METHODS

=over 2

=cut

=item B<new>

Constructor, returns an I<JSA::EnterData::SCUBA2> object.

    $scuba2 = new JSA::EnterData::SCUBA2();

Currently, no extra arguments are handled.

=cut

sub new {
    my ($class, %args) = @_;

    my $obj = $class->SUPER::new(%args) ;
    return bless $obj, $class;
}

=item B<get_bound_check_command>

Returns a list of command and its argument to be executed to
check/find the bounds.

    @cmd = $scuba2->get_bound_check_command;

    system(@cmd) == 0
        or die "Problem running the bound check command";

=cut

sub get_bound_check_command {
    my ($self, $fh, $pos_angle) = @_;

    my $smurf_dir = $ENV{'SMURF_DIR'};
    die 'SMURF_DIR not set' unless defined $smurf_dir;
    die 'SMURF_DIR does not exist' unless -d $smurf_dir;

    # Turn off autogrid; only rotate raster maps. Just need bounds.
    return (
        File::Spec->catfile($smurf_dir, 'makemap'),
        "in=^$fh",
        'system=ICRS',
        'out=!',
        'pixsize=1',
        'msg_filter=quiet',
        (defined $pos_angle ? "crota=$pos_angle" : ()),
        'method=ITER', 'config=!',
        'reset',
    );
}


=item B<instrument_name>

Returns the name of the instrument involved.

    $name = $scuba2->instrument_name();

=cut

sub instrument_name {
    return 'SCUBA-2';
}

=item B<instrument_table>

Returns the database table related to the instrument.

    $table = $scuba2->instrument_table();

=cut

sub instrument_table {
    return 'SCUBA2';
}

=item B<_do_verification>

Should we use JCMT::DataVerify?

=cut

sub _do_verification {
    my $self = shift;
    # XXX Skip badly needed data verification for SCUBA-2 until implemented.
    return 0;
}

=item B<raw_basename_regex>

Returns the regex to match base file name, with array, date and run
number captured ...

    qr/(s[48][a-d])
        (\d{8})
        _
        (\d{5})
        _\d{4}[.]sdf
      /x;

    $re = JSA::EnterData::SCUBA2->raw_basename_regex();

=cut

sub raw_basename_regex {
    return
        qr{ (s[48][a-d])  # array,
            (\d{8})       # date,
            _
            (\d{5})       # run number.
            _\d{4}[.]sdf
          }x;
}


=item B<raw_parent_dir>

Returns the parent directory of a raw file, without array, date, &
run number components.

    $root = JSA::EnterData::SCUBA2->raw_parent_dir();

=cut

sub raw_parent_dir {
    return '/jcmtdata/raw/scuba2';
}


=item B<make_raw_paths>

Given a list of base file names, returns a list of (unverified)
absolute paths.

    my @path = JSA::EnterData::SCUBA2->make_raw_paths(@basename);

=cut

sub make_raw_paths {
    my ($self, @base) = @_;

    return unless scalar @base;

    my $re   = $self->raw_basename_regex();
    my $root = $self->raw_parent_dir();

    my @path;
    foreach my $name (@base) {
        my ($array, $date, $run) = ($name =~ $re);
        next unless $array && $date && $run;

        push @path, File::Spec->catfile($root, $array, $date, $run, $name);
    }

    return @path;
}


=item B<transform_header>

Given a header hash reference, returns a hash reference of
headers.  The returned headers have subheaders
with subarray appended.

    $header = $scuba2->transform_header(\%header);

=cut

sub transform_header {
    my ($self, $header) = @_;

    my %new;
    for my $k (keys %$header) {
        next if lc $k eq 'subheaders';
        $new{$k} = $header->{$k};
    }

    $self->append_array_column(\%new, $header);

    my $subh = exists $header->{'SUBHEADERS'} ? $header->{'SUBHEADERS'} : [];

    for my $s (@{$subh}) {
        $self->append_array_column(\%new, $s);

        # Copy last value of BASETEMP, BBHEAT to main header.
        $self->push_header(\%new, $s, qr/^(?:BASETEMP|BBHEAT)$/);
    }

    my $skip;
    # Don't skip darks if it is a noise or a flat field, as shutter remains
    # closed for those.
    for my $key (qw/OBS-TYPE OBS_TYPE/) {
        next unless exists $header->{$key};

        $skip = $header->{$key} !~ /\b(?: noise | flat.?field )/xi;

        last;
    }

    # Special handling for /date.(obs|end)/ & /ms(start|end)/.
    $self->push_extreme_start_end(\%new, $subh);

    $self->push_date_obs_end(\%new, $subh);

    $self->push_range_headers_to_main(\%new, $subh, $skip);

    # Determine integration time.  This is computed as total (double-counting
    # by number of subarrays) divided by number of subarrays to allow for
    # incremental updates which may feature data from different numbers of
    # subarrays.
    do {
        my $total = $self->get_total_int_time($header);
        my $mult = $self->get_subarray_count([\%new]);
        $new{'INT_TIME'} = ($mult > 0) ? ($total / $mult) : 0.0;
    };

    return \%new;
}

=item B<get_total_int_time>

Extract the total of the INT_TIME header values for headers
corresponding to the main part of the observation.

Note: this double-counts time spent integrating between the various
sub-arrays.  Use L<get_subarray_count> to determine the corresponding
factor.

=cut

sub get_total_int_time {
    my ($self, $header) = @_;

    my $subh = exists $header->{'SUBHEADERS'} ? $header->{'SUBHEADERS'} : [];

    # Re-use the logic from 'calcbounds_update_bound_cols' that the main part
    # is that where OBS_TYPE = SEQ_TYPE.
    my $obs_type = $self->_find_header(
        headers => $header, name => 'OBS_TYPE', value => 1);

    my $total = 0.0;

    foreach my $s ($header, @$subh) {
        my $seq_type = exists $s->{'SEQ_TYPE'} ? $s->{'SEQ_TYPE'} : $header->{'SEQ_TYPE'};
        my $int_time = exists $s->{'INT_TIME'} ? $s->{'INT_TIME'} : $header->{'INT_TIME'};
        $total += $int_time if defined $int_time and $seq_type eq $obs_type;
    }

    return $total;
}

=item B<get_subarray_count>

Extract the number of subarrays in use.

This should be applied to an array of "transformed" headers
(see L<transform_header>) as it uses the SUBARRAY_a/b/c/d headers.

=cut

sub get_subarray_count {
    my ($self, $headers) = @_;

    my %seen = ();

    foreach my $header (@$headers) {
        my $filter = undef;

        while (my ($key, $value) = each %$header) {
            $filter = $value if $key =~ /^filter$/i;
        }

        next unless defined $filter;

        while (my ($key, $value) = each %$header) {
            $seen{$filter . uc($key)} = 1 if $key =~ /^subarray_.$/i and $value;
        }
    }

    return scalar keys %seen;
}

=item B<combine_int_time>

Combine INT_TIME values from multiple "transformed" headers.

=cut

sub combine_int_time {
    my ($self, $headers) = @_;

    my $new_mult = $self->get_subarray_count($headers);
    return 0.0 unless $new_mult > 0;

    my $total = 0.0;

    foreach my $header (@$headers) {
        my $int_time = undef;

        while (my ($key, $value) = each %$header) {
            $int_time = $value if $key =~ /^int_time$/i;
        }

        next unless $int_time;

        my $mult = $self->get_subarray_count([$header]);

        $total += $int_time * $mult;
    }

    return $total / $new_mult;
}

BEGIN {
  my @seq = qw/SEQSTART SEQEND/;

=item B<get_end_subheaders>

Given an array reference of subheader hash references, returns two
hash references defining starting and ending subheaders: first and
last, based on C<SEQSTART> and C<SEQEND> respectively.
It optionally
takes a truth value to indicate if to skip darks.

    ($start, $end) = $scuba2->get_end_subheaders(\@subheaders);

    # Skip darks.
    ($start, $end) = $scuba2->get_end_subheaders(\@subheaders, 1);

=cut

    sub get_end_subheaders {
        my ($self, $subheaders, $skip_dark) = @_;

        my ($init, $start, $end);
        foreach my $h (@$subheaders) {
            next if $skip_dark && $self->_is_dark($h);

            next unless exists $h->{$seq[0]}
                     && exists $h->{$seq[1]};

            unless ($init) {
                $end = $start = $h;
                $init ++;
                next;
            }

            my ($k_start, $k_end) = map {$h->{$_}} @seq;

            $start = $h if defined $k_start && $start->{$seq[0]} >  $k_start;
            $end   = $h if defined $k_end   &&   $end->{$seq[1]} <= $k_end;
        }

        return ($start, $end);
    }

=item B<push_extreme_start_end>

Given a header hash reference and an subheader array reference, copies
the first true-value field in a subheader hash reference to the main
header.

    $scuba2->push_extreme_start_end(\%header, \@subheader);

For *START fields, search starts from the front; for *END, from the
end.  For the list of fields see I<_find_first_field>.

=cut

    sub push_extreme_start_end {
        my ($self, $head, $subheaders) = @_;

        return unless $subheaders
                   && scalar @{$subheaders};

        my @subh = grep {exists $_->{$seq[0]} && exists $_->{$seq[1]}}
                        @{$subheaders}
            or return;

        my @start = sort {$a->{$seq[0]} <=> $b->{$seq[0]}} @subh;

        my @end = sort {$b->{$seq[1]} <=> $a->{$seq[1]}} @subh;

        $self->_find_first_field($head, \@start);
        $self->_find_first_field($head, \@end, my $end = 1);
    }

=item B<push_range_headers_to_main>

Given a header & a subheader hash references, moves range-type fields
from the subheader into the main header.  It optionally takes a truth
value to indicate if to skip darks.

    $scuba2->push_range_headers_to_main($header, $_)
        for @{$header->{'SUBHEADERS'}};

Currently, the fields being moved are those defined in JSA::DB::TableCOMMON.

=cut

    # Note: this doesn't include DATE-OBS and DATE-END because the
    # column names are date_obs and date_end -- those are handled separately.
    my $start_re = join '|', JSA::DB::TableCOMMON::range_start_columns();
    my $end_re = join '|', JSA::DB::TableCOMMON::range_end_columns();
    $_ = qr/(?:$_)/ix for $start_re, $end_re;

    sub push_range_headers_to_main {
        my ($self, $header, $subhead, $skip_dark) = @_;

        return if 1 >= scalar @{$subhead};

        my ($start, $end) = $self->get_end_subheaders($subhead, $skip_dark);

        $self->push_header($header, $start, $start_re) if $start;
        $self->push_header($header, $end, $end_re) if $end;
    }

=item B<_find_first_field>

Given an array reference of subheaders, a hash reference as storage,
copies the I<first> field existing in a subheader (value of which
evalutes to true) to the storage hash reference. It takes an optional
truth value to select the list of field names.  Default list is ...

    AMSTART
    AZSTART
    ELSTART

If the optional value is true, then list consists of ...

    AMEND
    AZEND
    ELEND


    # For all the fields in default list, copy to C<%$head>.
    $scuba2->_find_first_field($head, \@subheader_a);

    # Select alternative list.
    $scuba2->_find_first_field($head, \@subheader_b, 1);

=cut

    my @extreme_start = qw/
        AMSTART
        AZSTART
        ELSTART
    /;

    my @extreme_end = qw/
        AMEND
        AZEND
        ELEND
    /;

    sub _find_first_field {
        my ($self, $head, $subheaders, $choose_end) = @_;

        return unless $subheaders
                   && scalar @{$subheaders};

        my @field = ! $choose_end ? @extreme_start : @extreme_end;

        my $saved;

        foreach my $sub (@{$subheaders}) {
            foreach my $f (@field) {
                next unless exists $sub->{$f}
                         && defined $sub->{$f};

                $head->{$f} = $sub->{$f};
                $saved ++;
            }
            last if $saved;
        }
    }
}

=item B<push_date_obs_end>

Given a main header hash reference and an array reference of
subheaders, copies C<DATE-OBS> & C<DATE-END> fields from
subheaders to the main header.

    $scuba2->push_date_obs_end(\%header, \@subheader);

=cut

sub push_date_obs_end {
    my ($self, $header, $subheaders) = @_;

    return unless $subheaders
               && scalar @{$subheaders};

    my ($start, $end) = ('DATE-OBS', 'DATE-END');
    my (@start, @end);

    foreach my $sub (@{$subheaders}) {
        my ($s, $e) = map {exists $sub->{$_} ? $sub->{$_} : ()} ($start, $end);

        push @start, $s if $s;
        push @end,   $e if $e;
    }

    my (%new, $alt_end);
    ($new{$start}, $alt_end) = (sort @start)[0, -1];
    $new{$end} = (sort @end)[-1] // $alt_end;

    return $self->push_header($header, \%new);
}

=item B<push_header>

It is a general purpose method to copy fields to given header hash
reference from another given hash reference.  It optionally takes a
regular expression to filter out the keys in of subheader.

    $scuba2->push_header(\%header,
                         {'DATE-OBS' => '20091003T00:00:00',
                          'DATE-END' => '20091003T11:11:11'
                         });


    # Copy only DATE* fields.
    $scuba2->push_header(\%header,
                         {'DATE-OBS' => '20091003T00:00:00',
                          'DATE-END' => '20091003T11:11:11',
                          'AMSTART'  => '20091003T01:00:00'
                         },
                         'DATE');

=cut

sub push_header {
    my ($self, $header, $sub, $re) = @_;

    foreach my $key (keys %{$sub}) {
        next if $re && $key !~ /$re/;

        unless (exists $header->{$key}
                && defined $header->{$key}
                && ! defined $sub->{$key}) {
            $header->{$key} = $sub->{$key};
        }
    }

    return;
}

# If shutter field is '1.0', it is open|not dark (else, it is '0.0' & is closed|dark).
sub _is_dark {
    my ($class, $subhead) = @_;

    return unless exists $subhead->{'SHUTTER'}
               && defined $subhead->{'SHUTTER'};

    return ! ($subhead->{'SHUTTER'} + 0);
}

=item B<_fill_headers_obsid_subsys>

Fills "obsid_subsysnr" value.

=cut

sub _fill_headers_obsid_subsys {
    my ($self, $header, $obsid) = @_;

    $header->{'obsid_subsysnr'} = $self->_find_header(
        headers => $header, name => 'OBSIDSS', value => 1);
}

=item B<append_array_column>

Returns nothing.  Given a (sub)header hash reference, appends some of
the fields with C<_[a-d]> as appropriate to the given new header hash.

Throws L<JSA::Error> exception if a matching C<SUBARRAY> field value
not found.

   $scuba2->append_array_column(\%new, $subheader);

=cut

sub append_array_column {
    my ($self, $header, $sub) = @_;

    return unless $sub->{'SUBARRAY'} ;

    # Table column names suffixed by with [a-d].
    my @variation = qw/
        ARRAYID
        DETBIAS
        FLAT
        PIXHEAT
        SUBARRAY
    /;

    my $subarray_col = qr/([a-d])/i;
    my ($col) = $sub->{'SUBARRAY'} =~ $subarray_col
        or throw JSA::Error "No match found for subarray";

    foreach my $field (@variation) {
        next unless exists $sub->{$field};

        my $alt = join '_', $field, $col;

        # Need to change subarray_[a-d] to a bit value.
        if ($field eq 'SUBARRAY') {
            $header->{$alt} = 1;
        }
        else {
            $header->{$alt} = $sub->{$field};
        }
    }

    return;
}


=item B<fill_headers_FILES>

Fills in the headers for C<FILES> database table, given a headers hash
reference and an L<OMP::Info::Obs> object.

    $scuba2->fill_headers_FILES(\%header, $obs);

This first calls the superclass method L<fill_headers_FILES/JSA::EnterData>.

=cut

sub fill_headers_FILES {
    my $self = shift;
    my ($header, undef, undef, undef) = @_;

    # Call the superclass version of this method.
    $self->SUPER::fill_headers_FILES(@_);

    my @file = @{$header->{'file_id'}};

    unless (exists $header->{'obsid_subsysnr'}) {
        if (exists $header->{'OBSIDSS'}) {
            $header->{'obsid_subsysnr'} = $header->{'OBSIDSS'};
        }
        elsif (exists $header->{'SUBHEADERS'}) {
            foreach my $subh (@{$header->{'SUBHEADERS'}}) {
                push @{$header->{'obsid_subsysnr'}}, $subh->{'OBSIDSS'};
            }
        }
    }

    # Add 'subsysnr' field.
    my $parse = qr/^ s([48]) [a-d] /xi;

    for my $i (0 .. scalar @file - 1) {
        next if $header->{'subsysnr'}[$i];

        (my $wavelen) = $file[$i] =~ $parse
            or next;

        $header->{'subsysnr'}[$i] = "${wavelen}50";
    }

    return;
}

1;

=pod

=back

=head1 AUTHORS

=over 2

=item *

Anubhav E<lt>a.agarwal@jach.hawaii.eduE<gt>

=back

Copyright (C) 2008, Science and Technology Facilities Council.
All Rights Reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or (at
your option) any later version.

This program is distributed in the hope that it will be useful,but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place,Suite 330, Boston, MA  02111-1307,
USA

=cut
