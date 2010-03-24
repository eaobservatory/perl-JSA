package JSA::EnterData::SCUBA2;

use strict;
use warnings;

use Carp qw[ carp ];
use List::Util qw[ first ];

use base 'JSA::EnterData::Instrument';

=head1 NAME

JSA::EnterData::SCUBA2 - SCUBA2 specific methods.

=head1 SYNOPSIS

  # Create new object, with specific header dictionary.
  my $scuba2 = JSA::EnterData::SCUBA2->new

  my $name = $scuba2->name;

  my @cmd = $scuba2->get_bound_check_command;
  system( @cmd ) == 0
    or die "Problem with running bound check command for $name.";

  # Use table in a SQL later.
  my $table = $scuba2->table;


=head1 DESCRIPTION

JAS::EnterData::SCUBA2 is a object oriented module, having instrument specific
methods in order to be called from L<JSA::EnterData>.

=head2 METHODS

=over 2

=cut

=item B<new>

Constructor, returns an I<JSA::EnterData::SCUBA2> object.

  $scuba2 = JSA::EnterData::SCUBA2->new;

Currently, no extra arguments are handled.

=cut

sub new {

  my ( $class, %args ) = @_;

  my $obj = $class->SUPER::new( %args ) ;
  return bless $obj, $class;
}

=item B<calc_freq>

Does nothing currently.

=cut

sub calc_freq {

  my ( $self ) = @_;

  return;
}

=item B<get_bound_check_command>

Returns a list of command and its argument to be executed to
check/find the bounds.

  @cmd = $scuba2->get_bound_check_command;

  system( @cmd ) == 0
    or die "Problem running the bound check command";

=cut

sub get_bound_check_command {

  my ( $self, $fh, $pos_angle ) = @_;

  # Turn off autogrid; only rotate raster maps. Just need bounds.
  return
    ( '/star/bin/smurf/makemap',
      "in=^$fh",
      'system=ICRS',
      'out=!',
      'pixsize=1',
      'msg_filter=quiet',
      ( defined $pos_angle ? "crota=$pos_angle" : () ),
      'method=REBIN',
      'reset'
    );
}


=item B<name>

Returns the name of the instrument involved.

  $name = $scuba2->name;

=cut

sub name { return 'SCUBA-2' ; }


=item B<name_is_scuba2>

Returns a truth value indicating if the given string matches some variation of
"SCUBA-2".

  #  Prints "matched".
  print 'matched'
    if $scuba2->name_is_scuba2( 'scuba2' ) ;

Purpose of it is to reduce the number of times to reproduce same regular
expression test.

=cut

# There could be a better place for this method than here.
sub name_is_scuba2 {

  my ( $class, $name ) = @_;

  my $re = qr{^scuba-?2$}i;

  return $name =~ $re;
}

=item B<table>

Returns the database table related to the instrument.

  $table = $scuba2->table;

=cut

sub table { return 'SCUBA2'; }

BEGIN {

  my $obsidss_re = qr{^ obsidss $}xi;
  my $subarray_re = qr{^ subarray (?:_[a-d])? $}xi;

=item B<transform_header>

Given a header hash reference, returns an array of hash references of
headers grouped by subarray.  The returned headers have subheaders
with subarray appended, "obsid_subsynr" field filled in (see
I<transform_subheader> method).

  @grouped = $scuba2->transform_header( \%header );

=cut

  sub transform_header {

    my ( $self, $header ) = @_;

    $self->append_array_column( $header );

    my @fields = keys %{ $header };

    my $header_val =
      sub {
        my ( $filter ) = @_;
        return ( map { /$filter/ ? $header->{ $_ } : () } @fields )[0];
      };

    my $header_obsidss = $header_val->( $obsidss_re );

    my $subh =
      exists $header->{'SUBHEADERS'}
      ? $header->{'SUBHEADERS'}
      : ();

    for my $s ( @{ $subh } ) {

      $self->append_array_column( $s );

      $s->{'obsid_subsysnr'} ||= $s->{'OBSIDSS'} || $header_obsidss;
    }

    my %new;
    for my $k ( @fields ) {

      next if lc $k eq 'subheaders';
      $new{ $k } = $header->{ $k };
    }

    my $skip;
    # Don't skip darks if it is a noise or a flat field, as shutter remains
    # closed for those.
    for my $key ( qw[ OBS-TYPE OBS_TYPE ] ) {

      next unless exists $header->{ $key };

      $skip = $header->{ $key } !~ m{\b(?: noise | flat.?field )}xi;
      last;
    }

    # Special handling for /date.(obs|end)/ & /ms(start|end)/.
    $self->push_extreme_start_end( \%new, $subh );

    $self->push_date_obs_end( \%new, $subh, $skip );

    $self->push_range_headers_to_main( \%new, $subh, $skip );

    my $grouped =
      $self->group_by_subarray( $subh, $header_val->( $subarray_re ) );

    my @new;
    for my $sa ( keys %{ $grouped } ) {

      push @new, { %{ $grouped->{ $sa } } , %new };
    }

    return ( \%new , \@new );
  }
}

BEGIN {

  my @seq = qw[ SEQSTART SEQEND ];

=item B<get_end_subheaders>

Given an array reference of subheader hash references, returns two
hash references defining starting and ending subheaders: first and
last, based on C<SEQSTART> and C<SEQEND> respectively.
It optionally
takes a truth value to indicate if to skip darks.

  ( $start, $end ) = $scuba2->get_end_subheaders( \@subheaders );

  # Skip darks.
  ( $start, $end ) = $scuba2->get_end_subheaders( \@subheaders, 1 );

=cut

  sub get_end_subheaders {

    my ( $self, $subheaders, $skip_dark ) = @_;

    my ( $init, %start, %end );
    for my $h ( @{ $subheaders } ) {

      next
        if $skip_dark
        && $self->_is_dark( $h );

      my %h = %{ $h };

      next
        unless exists $h{ $seq[0] }
        && exists $h{ $seq[1] };

      my ( $k_start, $k_end ) = map { $h{ $_ } } @seq;

      unless ( $init ) {

        %end = %start = %h;
        $init++;
        next;
      }

      %start = %h if $start{ $seq[0] } >  $k_start;
      %end   = %h if $end{ $seq[1] }   <= $k_end;
    }

    return ( { %start }, { %end } );
  }

=item B<push_extreme_start_end>

Given a header hash reference and an subheader array reference, copies
the first true-value field in a subheader hash reference to the main
header.

  $scuba2->push_extreme_start_end( \%header, \@subheader );

For *START fields, search starts from the front; for *END, from the
end.  For the list of fields see I<_find_first_field>.

=cut

  sub push_extreme_start_end {

    my ( $self, $head, $subheaders ) = @_;

    return
      unless $subheaders
      && scalar @{ $subheaders };

    my @subh =
          grep
          { exists $_->{ $seq[0] } && exists $_->{ $seq[1] } } @{ $subheaders }
          or return ;

    my @start =
      sort { $a->{ $seq[0] } <=> $b->{ $seq[0] } }
      @subh;

    my @end =
      sort { $b->{ $seq[1] } <=> $a->{ $seq[1] } }
      @subh;

    my %new;
    $self->_find_first_field( $head, \@start, \%new );
    $self->_find_first_field( $head, \@end, \%new, my $end = 1 );

    return
      $self->push_header( $head, { %new } );
  }

=item B<push_range_headers_to_main>

Given a header & a subheader hash references, moves range-type fields
from the subheader into the main header.  It optionally takes a truth
value to indicate if to skip darks.

  $scuba2->push_range_headers_to_main( $header, $_ )
    for @{ $header->{'SUBHEADERS'} };

Currently, the fields being moved are ...

  ATSTART AZSTART
  BKLEGTST BPSTART
  FRLEGTST
  HSTSTART HUMSTART
  SEEDATST SEEINGST SEQSTART
  TAU225ST TAUDATST
  WNDDIRST WNDSPDST WVMDATST WVMTAUST

  ATEND AZEND
  BKLEGTEN BPEND
  FRLEGTEN
  HSTEND HUMEND
  SEEDATEN SEEINGEN SEQEND
  TAU225EN TAUDATEN
  WNDDIREN WNDSPDEN WVMDATEN WVMTAUEN

=cut

  my @start_rest =
    qw[
        ATSTART AZSTART
        BKLEGTST BPSTART
        FRLEGTST
        HSTSTART HUMSTART
        MSSTART
        SEEDATST SEEINGST SEQSTART
        TAU225ST TAUDATST
        WNDDIRST WNDSPDST WVMDATST WVMTAUST
    ];

  my @end_rest =
    qw[
        ATEND AZEND
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
  $_ = qr{(?:$_)}ix for $start_re, $end_re;

  sub push_range_headers_to_main {

    my ( $self, $header, $subhead, $skip_dark ) = @_;

    return if 1 >= scalar @{ $subhead };

    my ( $start, $end ) = $self->get_end_subheaders( $subhead, $skip_dark );

    $self->push_header( $header, $start, $start_re ) if $start;
    $self->push_header( $header, $end, $end_re ) if $end;

    return;
  }

=item B<_find_first_field>

Given an array reference of subheaders, a hash reference as storage,
copies the I<first> field existing in a subheader (value of which
evalutes to true) to the storage hash reference. It takes an optional
truth value to select the list of field names.  Default list is ...

  AMSTART

If the optional value is true, then list consists of ...

  AMEND


  # For all the fields in default list, copy to C<%save>.
  $scuba2->_find_first_field( \@subheader_a, \%save, );

  # Select alternative list.
  $scuba2->_find_first_field( \@subheader_b, \%save, 1 );

=cut

  my @extreme_start =
    qw[
        AMSTART
      ];

  my @extreme_end =
    qw[
        AMEND
      ];

  sub _find_first_field {

    my ( $self, $head, $subheaders, $save, $choose_end ) = @_;

    return
      unless $subheaders
      && scalar @{ $subheaders };

    my @field = $choose_end ? @extreme_start : @extreme_end;

    SUBHEADER:
    for my $sub ( @{ $subheaders } ) {

      FIELD:
      for my $f ( @field ) {

        next FIELD
          unless exists $sub->{ $f }
          && $sub->{ $f }
          ;

        $save->{ $f } =  $sub->{ $f };
        last FIELD;
      }
    }

    return
      $self->_find_in_main_header( $head, $save, \@field );
  }


}

=item B<_find_in_main_header>

Purpose is to save the headers/fields found in main header after
failing to find headers/fields in C<SUBHEADERS> hash reference.  Only
plain vlaues or C<ARRAY> types (as defined by L<ref>) are handled.

  $self->_find_in_main_header( \%header, \%save, \@field_names );

=cut

sub _find_in_main_header {

  my ( $self, $head, $save, $fields ) = @_;

  # Search in main header now if not found in sub headers.
  for my $f ( @{ $fields } ) {

    next
      if exists $save->{ $f }
      || ! exists $head->{ $f };

    my $val = $head->{ $f };

    if ( ref $val && ref $val ne 'ARRAY' ) {

      carp( sprintf 'Do not know how to handle %s value of type %s in header.',
            $f,
            ref $val
          );
      next;
    }

    $save->{ $f } = first { $_ } ref $val ? @{ $val } : ( $val );
  }

  return;
}

=item B<push_date_obs_end>

Given a main header hash reference and an array reference of
subheaders, copies C<DATE-OBS> & C<DATE-END> fields from "darks"
(subheaders) to the main header.

  $scuba2->push_date_obs_end( \%header, \@subheader );

At least two darks are expected.  For definition of dark, see
I<_is_dark>.

=cut

sub push_date_obs_end {

  my ( $self, $header, $subheaders, $skip_dark ) = @_;

  return
    unless $subheaders
    && scalar @{ $subheaders };

  my @dark;
  for my $sub ( @{ $subheaders } ) {

    next
      if $skip_dark
      && ! $self->_is_dark( $sub );

    push @dark, $sub;
  }

  my $dates =
    scalar @dark
    ? \@dark
    : # Plan B: use first & last subheaders for could not find dark
      # subheaders.
      [ map { @{ $_ }[0,-1] } $subheaders ]
      ;

  my %new;
  my ( $start, $end ) = ( 'DATE-OBS', 'DATE-END' );
  $new{ $_ } = $dates->[0]->{ $_ } for $start;
  $new{ $_ } = $dates->[-1]->{ $_ } || $new{ $start } for $end;

  return
    $self->push_header( $header, { %new } );
}

=item B<push_header>

It is a general purpose method to copy fields to given header hash
reference from another given hash reference.  It optionally takes a
regular expression to filter out the keys in of subheader.

  $scuba2->push_header( \%header,
                          { 'DATE-OBS' => '20091003T00:00:00',
                            'DATE-END' => '20091003T11:11:11'
                          }
                      );


  # Copy only DATE* fields.
  $scuba2->push_header( \%header,
                        { 'DATE-OBS' => '20091003T00:00:00',
                          'DATE-END' => '20091003T11:11:11',
                          'AMSTART'  => '20091003T01:00:00'
                        },
                        'DATE'
                      );

=cut

sub push_header {

  my ( $self, $header, $sub, $re ) = @_;

  for my $key ( keys %{ $sub } ) {

    next if $re && $key !~ m/$re/;

    if ( exists $header->{ $key }
          && defined $header->{ $key }
          && ! defined $sub->{ $key }
        ) {

      delete $sub->{ $key };
      next;
    }

    $header->{ $key } = $sub->{ $key };
    delete $sub->{ $key };
  }

  return;
}

# If shutter field is '1.0', it is open|not dark (else, it is '0.0' & is closed|dark).
sub _is_dark {

  my ( $class, $subhead ) = @_;

  return
    unless exists $subhead->{'SHUTTER'}
    && defined $subhead->{'SHUTTER'};

  return ! ( $subhead->{'SHUTTER'} + 0 );
}

=item B<group_by_subarray>

Returns a hash reference of hash references, where the sole key is the
subarray type matching C</^s[48].?$/>, given an array reference of
subheaders & optional subarray type.

Throws L<JSA::Error> exception if a subarray type cannot be determined.

 $grouped =
  $scuba2->group_by_subarray( $header->{'SUBHEADERS'}, 's4' );

=cut

sub group_by_subarray {

  my ( $self, $subheaders, $header_arr ) = @_;

  my $group = {};

  my $array_re = qr{^( s[48] .? )$}ix;

  my $array = $header_arr;

  my $int_key = 'INT_TIME';

  for my $sub ( @{ $subheaders } ) {

    my @keys = keys %{ $sub };

    unless ( $header_arr ) {
      for ( @keys ) {

        next if -1 == index lc $_, 'subarray';

        ( $array ) = ( $sub->{ $_ } =~ $array_re )
          and last;
      }
    }

    unless ( $array ) {

      throw JSA::Error
        sprintf "Failed to find subarray type. (Subheaders:\n%s)",
        _dump( $subheaders );
    }


    for ( @keys ) {

      # Collect fields unless already exist (except for integration time) ...
      unless ( $_ eq $int_key ) {

        $group->{ $array }{ $_ } ||= $sub->{ $_ };
      }
      # ... for integration time, sum all of them.
      else {

        $group->{ $array }{ $_ } += $sub->{ $_ };
      }
    }
  }

  return $group;
}

=item B<_fill_headers_obsid_subsys>

Does nothing. Field "obsid_subsysnr" is taken care by
I<transform_subheader> method.

=cut

#  transform_header() takes care of the obsid_subsynr value.
sub _fill_headers_obsid_subsys { }

=item B<append_array_column>

Returns nothing.  Given a (sub)header hash reference, appends some of
the fields with C<_[a-d]> as appropriate.

Throws L<JSA::Error> exception if a matching C<SUBARRAY> field value
not found.

 $scuba2->append_array_column( $subheader );

=cut

sub append_array_column {

  my ( $self, $header ) = @_;

  return unless  $header->{'SUBARRAY'} ;

  # Table column names suffixed by with [a-d].
  my @variation =
    qw[ ARRAYID
        DETBIAS
        FLAT
        PIXHEAT
        SUBARRAY
      ];

  my $subarray_col = qr{([a-d])}i;
  my ( $col ) = $header->{'SUBARRAY'} =~ $subarray_col
    or throw JSA::Error "No match found for subarray";

  for my $field ( @variation ) {

    next unless exists $header->{ $field };

    my $alt = join '_', $field, $col;

    $header->{ $alt } = $header->{ $field };

    # Need to change subarray_[a-d] to a bit value.
    if ( $field eq 'SUBARRAY' ) {

      $header->{ $alt } = $header->{ $alt } ? 1 : 0;
    }
  }

  return;
}


=item B<fill_headers_FILES>

Fills in the headers for C<FILES> database table, given a headers hash
reference and an L<OMP::Info::Obs> object.

  $scuba2->fill_headers_FILES( \%header, $obs );

It needs to be called after L<fill_headers_FILES/JSA::EnterData>.

=cut

sub fill_headers_FILES {

  my ( $self, $header, $obs ) = @_;

  # Add 'nsubscan' field.
  my $nsub = () = $header->{'NSUBSCAN'};

  my @file = @{ $header->{'file_id'} };

  if ( $nsub < scalar @file ) {

    $header->{'nsubscan'} =
      [ map { /_(\d+)[.]sdf$/ ? 0 + $1 : ()  } @{ $header->{'file_id'} } ];

  }

  if ( ! exists $header->{'obsid_subsysnr'} ) {

    if ( exists $header->{'OBSIDSS'} ) {

      $header->{'obsid_subsysnr'} = $header->{'OBSIDSS'};
    }
    elsif ( exists $header->{'SUBHEADERS'} ) {

      for my $subh ( @{ $header->{'SUBHEADERS'} } ) {

        push @{ $header->{'obsid_subsysnr'} }, $subh->{'OBSIDSS'};
      }
    }
  }

  # Add 'subsysnr' field.
  my $parse = qr{^ s([48]) [a-d] }xi;

  for my $i ( 0 .. scalar @file - 1 ) {

    next if $header->{'subsysnr'}[ $i ];

    ( my $wavelen ) =
      $file[ $i ] =~ $parse or next;

    $header->{'subsysnr'}[ $i ] = "${wavelen}50";
  }

  return;
}


=item B<fill_max_subscan>

Fills in the I<max_subscan> for C<SCUBA2> database table, given a
headers hash reference and an L<OMP::Info::Obs> object.

  $inst->fill_max_subscan( \%header, $obs );

=cut

sub fill_max_subscan {

  my ( $self, $header, $obs ) = @_;

  my $subh = 'SUBHEADERS';
  my $subar = 'SUBARRAY';
  my $max = 'max_subscan';

  if ( exists $header->{ $subh } ) {

    my %count;
    for ( @{ $header->{ $subh } } ) {

      $count{ $_->{ $subar } }++
        if exists $_->{ $subar };
    }

    for ( @{ $header->{ $subh } } ) {

      $_->{ $max } = $count{ $_->{ $subar } }
        if exists $_->{ $subar };
    }

    return if keys %count;
  }

  # In main header, there will be only one entry of subarry, so
  # nothing to count.
  $header->{ $max } = 1
    if ! $header->{ $max }
    && exists $header->{ $subar };

  return;
}

sub _dump {

  my ( @thing ) = @_;

  require Data::Dumper;

  local $Data::Dumper::Sortkeys = 1;
  local $Data::Dumper::Indent = 1;
  local $Data::Dumper::Deepcopy = 1;

  return
    Data::Dumper::Dumper( \@thing );
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

