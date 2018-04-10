package JSA::EnterData::SCUBA2;

use strict;
use warnings;

use Carp qw/carp/;
use Data::Dumper;
use File::Spec;
use List::Util qw/first/;

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

=item B<calc_freq>

Does nothing currently.

=cut

sub calc_freq {
  my ($self) = @_;

  return;
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

    # Turn off autogrid; only rotate raster maps. Just need bounds.
    return (
        '/star/bin/smurf/makemap',
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


BEGIN {
    my $obsidss_re = qr/^ obsidss $/xi;
    my $subarray_re = qr/^ subarray (?:_[a-d])? $/xi;

=item B<transform_header>

Given a header hash reference, returns an array of hash references of
headers grouped by subarray.  The returned headers have subheaders
with subarray appended, "obsid_subsysnr" field filled in.

    @grouped = $scuba2->transform_header(\%header);

=cut

    sub transform_header {
        my ($self, $header) = @_;

        $self->append_array_column($header);

        my @fields = keys %{$header};

        my $header_val = sub {
            my ($filter) = @_;
            return (map {/$filter/ ? $header->{$_} : ()} @fields)[0];
        };

        my $header_obsidss = $header_val->($obsidss_re);

        my $subh = exists $header->{'SUBHEADERS'}
                 ? $header->{'SUBHEADERS'}
                 : ();

        for my $s (@{$subh}) {
            $self->append_array_column($s);

            $s->{'obsid_subsysnr'} ||= $s->{'OBSIDSS'} || $header_obsidss;
        }

        my %new;
        for my $k (@fields) {
            next if lc $k eq 'subheaders';
            $new{$k} = $header->{$k};
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

        $self->push_date_obs_end(\%new, $subh, $skip);

        $self->push_range_headers_to_main(\%new, $subh, $skip);

        #@{$subh} = $self->merge_by_obsidss($subh);

        my $grouped = $self->group_by_subarray(
            $subh, $header_val->($subarray_re));

        my @new;
        for my $sa (keys %{$grouped}) {
            push @new, {%{$grouped->{$sa}} , %new};
        }

        return (\%new , \@new);
    }
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

        my ($init, %start, %end);
        foreach my $h (@{$subheaders}) {
            next if $skip_dark
                 && $self->_is_dark( $h );

            my %h = %{$h};

            next unless exists $h{$seq[0]}
                     && exists $h{$seq[1]};

            my ($k_start, $k_end) = map {$h{$_}} @seq;

            unless ($init) {
                %end = %start = %h;
                $init ++;
                next;
            }

            %start = %h if defined $k_start && $start{$seq[0]} >  $k_start;
            %end   = %h if defined $k_end   &&   $end{$seq[1]} <= $k_end;
        }

        return ({%start}, {%end});
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

        my %new;
        $self->_find_first_field($head, \@start, \%new);
        $self->_find_first_field($head, \@end, \%new, my $end = 1);

        return $self->push_header($head, {%new});
    }

=item B<push_range_headers_to_main>

Given a header & a subheader hash references, moves range-type fields
from the subheader into the main header.  It optionally takes a truth
value to indicate if to skip darks.

    $scuba2->push_range_headers_to_main($header, $_)
        for @{$header->{'SUBHEADERS'}};

Currently, the fields being moved are ...

    ATSTART
    BKLEGTST BPSTART
    FRLEGTST
    HSTSTART HUMSTART
    SEEDATST SEEINGST SEQSTART
    TAU225ST TAUDATST
    WNDDIRST WNDSPDST WVMDATST WVMTAUST

    ATEND
    BKLEGTEN BPEND
    FRLEGTEN
    HSTEND HUMEND
    SEEDATEN SEEINGEN SEQEND
    TAU225EN TAUDATEN
    WNDDIREN WNDSPDEN WVMDATEN WVMTAUEN

=cut

    my @start_rest = qw[
        ATSTART
        BKLEGTST BPSTART
        FRLEGTST
        HSTSTART HUMSTART
        MSSTART
        SEEDATST SEEINGST SEQSTART
        TAU225ST TAUDATST
        WNDDIRST WNDSPDST WVMDATST WVMTAUST
    ];

    my @end_rest = qw[
        ATEND
        BKLEGTEN BPEND
        FRLEGTEN
        HSTEND HUMEND
        MSEND
        SEEDATEN SEEINGEN SEQEND
        TAU225EN TAUDATEN
        WNDDIREN WNDSPDEN WVMDATEN WVMTAUEN
    ];

    my $start_re = join '|' , @start_rest;
    my $end_re = join '|', @end_rest;
    $_ = qr/(?:$_)/ix for $start_re, $end_re;

    sub push_range_headers_to_main {
        my ($self, $header, $subhead, $skip_dark) = @_;

        return if 1 >= scalar @{$subhead};

        my ($start, $end) = $self->get_end_subheaders($subhead, $skip_dark);

        $self->push_header($header, $start, $start_re) if $start;
        $self->push_header($header, $end, $end_re) if $end;

        return;
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


    # For all the fields in default list, copy to C<%save>.
    $scuba2->_find_first_field(\@subheader_a, \%save,);

    # Select alternative list.
    $scuba2->_find_first_field(\@subheader_b, \%save, 1);

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
        my ($self, $head, $subheaders, $save, $choose_end) = @_;

        return unless $subheaders
                   && scalar @{$subheaders};

        my @field = ! $choose_end ? @extreme_start : @extreme_end;

        my $saved;

        SUBHEADER: foreach my $sub (@{$subheaders}) {
            FIELD: foreach my $f (@field) {
                next FIELD unless exists $sub->{$f}
                               && defined $sub->{$f};

                $save->{$f} =  $sub->{$f};
                $saved ++;
            }
            last if $saved;
        }

        return $self->_find_in_main_header($head, $save, \@field);
    }
}

=item B<_find_in_main_header>

Purpose is to save the headers/fields found in main header after
failing to find headers/fields in C<SUBHEADERS> hash reference.  Only
plain vlaues or C<ARRAY> types (as defined by L<ref>) are handled.

    $self->_find_in_main_header(\%header, \%save, \@field_names);

=cut

sub _find_in_main_header {
    my ($self, $head, $save, $fields) = @_;

    # Search in main header now if not found in sub headers.
    for my $f (@{$fields}) {
        next if exists $save->{$f}
             || ! exists $head->{$f};

        my $val = $head->{$f};

        if (ref $val && ref $val ne 'ARRAY') {
            carp(sprintf
                'Do not know how to handle %s value of type %s in header.',
                $f, ref $val);
            next;
        }

        $save->{$f} = first {$_} ref $val ? @{$val} : ($val);
    }

    return;
}

=item B<push_date_obs_end>

Given a main header hash reference and an array reference of
subheaders, copies C<DATE-OBS> & C<DATE-END> fields from "darks"
(subheaders) to the main header.

    $scuba2->push_date_obs_end(\%header, \@subheader);

At least two darks are expected.  For definition of dark, see
I<_is_dark>.

=cut

sub push_date_obs_end {
    my ($self, $header, $subheaders, $skip_dark) = @_;

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
    $new{$end}   = (sort @end)[-1];
    for ($new{ $end}) {
        $_ = $alt_end unless defined $_;
    }

    return $self->push_header($header, {%new});
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
                         { 'DATE-OBS' => '20091003T00:00:00',
                          'DATE-END' => '20091003T11:11:11',
                          'AMSTART'  => '20091003T01:00:00'
                         },
                         'DATE');

=cut

sub push_header {
    my ($self, $header, $sub, $re) = @_;

    foreach my $key (keys %{$sub}) {
        next if $re && $key !~ /$re/;

        if (exists $header->{$key}
                && defined $header->{$key}
                && ! defined $sub->{$key}) {
            delete $sub->{ $key };
            next;
        }

        $header->{$key} = $sub->{$key};
        delete $sub->{$key};
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

=item B<group_by_subarray>

Returns a hash reference of hash references, where the sole key is the
subarray type matching C</^s[48].?$/>, given an array reference of
subheaders & optional subarray type.

Throws L<JSA::Error> exception if a subarray type cannot be determined.

    $grouped = $scuba2->group_by_subarray($header->{'SUBHEADERS'}, 's4');

=cut

sub group_by_subarray {
    my ($self, $subheaders, $header_arr) = @_;

    return unless $subheaders
               && scalar @{$subheaders};

    my $group = {};

    my $array_re = qr/^( s[48] .? )$/ix;

    my $array = $header_arr;

    my $int_key = 'INT_TIME';

    foreach my $sub (@{$subheaders}) {
        my @keys = keys %{$sub};

        unless ($header_arr) {
            for (@keys) {
                next if -1 == index lc $_, 'subarray';

                ($array) = ($sub->{$_} =~ $array_re)
                    and last;
            }
        }

        unless ($array) {
            throw JSA::Error
              sprintf "Failed to find subarray type. (Subheaders:\n%s)",
                      _dump($subheaders);
        }


        foreach (@keys) {
            # Collect fields unless already exist (except for integration time) ...
            unless ($_ eq $int_key) {
                $group->{$array}{$_} ||= $sub->{$_};
            }

            # ... for integration time, sum all of them.
            else {
                $group->{$array}{$_} += $sub->{$_};
            }
        }
    }

    return $group;
}

=item B<merge_by_obsidss>

Returns an array reference of hash references grouped by
C<obsid_subsysnr>, given an array reference of subheaders.

Throws L<JSA::Error> exception if a hash reference is missing
C<obsid_subsys>.

   @grouped = $scuba2->merge_by_obsidss($header->{'SUBHEADERS'});

=cut

sub merge_by_obsidss {
    my ($self, $subh) = @_;

    return $subh unless $subh
                     && scalar @{$subh};

    my (%obsidss, @order);
    my $key_re = qr/^ OBSID (?:_SUBSYS(?:NR)? | SS ) $/xi;

    for my $href (@{$subh}) {
        my $key = (grep {$_ =~ $key_re ? $_ : ()} keys %{$href})[0]
            or next;

        my $id = $href->{$key};
        my $old = $obsidss{$id};

        push @order, $id
            unless exists $obsidss{$id};

        %{$obsidss{$id}} = ((defined $old ? %{$old} : ()),
                            %{$href});
    }

    return [@obsidss{@order}];
}

=item B<_fill_headers_obsid_subsys>

Does nothing. Field "obsid_subsysnr" is taken care by
I<transform_header> method.

=cut

#  transform_header() takes care of the obsid_subsysnr value.
sub _fill_headers_obsid_subsys { }

=item B<append_array_column>

Returns nothing.  Given a (sub)header hash reference, appends some of
the fields with C<_[a-d]> as appropriate.

Throws L<JSA::Error> exception if a matching C<SUBARRAY> field value
not found.

   $scuba2->append_array_column($subheader);

=cut

sub append_array_column {
    my ($self, $header) = @_;

    return unless $header->{'SUBARRAY'} ;

    # Table column names suffixed by with [a-d].
    my @variation = qw/
        ARRAYID
        DETBIAS
        FLAT
        PIXHEAT
        SUBARRAY
    /;

    my $subarray_col = qr/([a-d])/i;
    my ($col) = $header->{'SUBARRAY'} =~ $subarray_col
        or throw JSA::Error "No match found for subarray";

    foreach my $field (@variation) {
        next unless exists $header->{$field};

        my $alt = join '_', $field, $col;

        $header->{$alt} = $header->{$field};

        # Need to change subarray_[a-d] to a bit value.
        if ($field eq 'SUBARRAY') {
            $header->{$alt} = $header->{$alt} ? 1 : 0;
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
    my ($self, $header, $obs) = @_;

    # Call the superclass version of this method.
    $self->SUPER::fill_headers_FILES($header, $obs);

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

sub _dump {
    my (@thing) = @_;

    local $Data::Dumper::Sortkeys = 1;
    local $Data::Dumper::Indent = 1;
    local $Data::Dumper::Deepcopy = 1;

    return Data::Dumper::Dumper(\@thing);
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
