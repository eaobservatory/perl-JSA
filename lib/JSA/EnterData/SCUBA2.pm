package JSA::EnterData::SCUBA2;

use strict;
use warnings;

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

    my $subh = $header->{'SUBHEADERS'};

    for my $s ( @{ $subh } ) {

      $self->append_array_column( $s );

      $s->{'obsid_subsysnr'} ||= $s->{'OBSIDSS'} || $header_obsidss;
    }

    my %new;
    for my $k ( @fields ) {

      next if lc $k eq 'subheaders';
      $new{ $k } = $header->{ $k };
    }

    # Speical handling for /date.(obs|end)/ & /ms(start|end)/.
    $self->push_ms_ends( \%new, $subh );
    $self->push_date_obs_end( \%new, $subh );

    # General purpose.
    $self->push_range_headers_to_main( \%new, $subh );

    my $grouped =
      $self->group_by_subarray( $subh, $header_val->( $subarray_re ) );

    # Now that headers are grouped by array, calcualte total integration time.
    $self->push_integration_time( \%new, $gropued{ (keys %{ $grouped })[0] } );

    my @new;
    for my $sa ( keys %{ $grouped } ) {

      push @new, { %{ $grouped->{ $sa } } , %new };
    }

    return ( \%new , \@new );
  }
}

BEGIN {

  my @start =
    qw[
        AMSTART ATSTART AZSTART
        BKLEGTST BPSTART
        DATE-OBS
        FRLEGTST
        HSTSTART HUMSTART
        MSSTART
        SEEDATST SEEINGST SEQSTART
        TAU225ST TAUDATST
        WNDDIRST WNDSPDST WVMDATST WVMTAUST
    ];

  my @end =
    qw[
        AMEND ATEND AZEND
        BKLEGTEN BPEND
        DATE-END
        FRLEGTEN
        HSTEND HUMEND
        MSEND
        SEEDATEN SEEINGEN SEQEND
        TAU225EN TAUDATEN
        WNDDIREN WNDSPDEN WVMDATEN WVMTAUEN
      ];

  my $start_re = join '|' , @start;
  my $end_re = join '|', @end;
  $_ = qr{(?:$_)}ix for $start_re, $end_re;

=item B<push_range_headers_to_main>

Given a header & a subheader hash references, moves range-type fields
from the subheader into the main header.

  $scuba2->( $header, $_ ) for @{ $header->{'SUBHEADERS'} };

Currently, the fields being moved are ...

  AMSTART ATSTART AZSTART
  BKLEGTST BPSTART
  DATE-OBS
  FRLEGTST
  HSTSTART HUMSTART
  SEEDATST SEEINGST SEQSTART
  TAU225ST TAUDATST
  WNDDIRST WNDSPDST WVMDATST WVMTAUST

  AMEND ATEND AZEND
  BKLEGTEN BPEND
  DATE-END
  FRLEGTEN
  HSTEND HUMEND
  SEEDATEN SEEINGEN SEQEND
  TAU225EN TAUDATEN
  WNDDIREN WNDSPDEN WVMDATEN WVMTAUEN

=cut

  sub push_range_headers_to_main {

    my ( $self, $header, $subhead ) = @_;

    #return if 1 >= scalar @{ $subhead };

    my $extract =
    sub {
      my ( $sub, $re ) = @_;
      for my $key ( keys %{ $sub } ) {

        next unless $key =~ $re;

        if ( exists $header->{ $key } ) {

          delete $sub->{ $key };
          next;
        }

        $header->{ $key } = $sub->{ $key };
        delete $sub->{ $key };
      }
      return;
    };

    my ( $start, $end ) = $self->get_end_subheaders( $subhead );

    $extract->( $start, $start_re ) if $start;
    $extract->( $end, $end_re ) if $end;

    return;
  }
}

sub push_integration_time {

  my ( $self, $header, $subarray ) = @_;

}

sub push_date_obs_end {

  my ( $self, $header, $subheaders ) = @_;

  my @dark;
  for my $sub ( @{ $subheaders } ) {

    next unless $self->_is_dark( $sub );

    push @dark, $sub;
  }

  my ( $start, $end ) = ( 'DATE-OBS', 'DATE-END' );
  my %new;
  $new{ $_ } = $dark[0]->{ $_ } for $start;
  $new{ $_ } = $dark[1]->{ $_ } || $new{ $start } for $end;

  return
    $self->push_range_headers_to_main( $head, [ { %new } ] );
}

sub push_ms_ends {

  my ( $self, $head, $subheaders ) = @_;

  my ( $start, $end ) = ( 'MSSTART', 'MSEND' );
  my %new;
  for my $sub ( @{ $subheaders } ) {

    last if 2 == scalar keys %new;

    $new{ $start } = $sub->{ $start }
      if ! exists $new{ start }
      && exists $sub->{ $start };

    $new{ $end } = $sub->{ $end }
      if ! exists $new{ $end }
      && exists $sub->{ $end };
  }

  return
    $self->push_range_headers_to_main( $head, [ { %new } ] );
}

=item B<get_end_subheaders>

Given an array reference of subheader hash references, returns two hash
references defining starting and ending subheaders

  ( $start, $end ) = $scuba2->get_end_subheaders( \@subheaders );

=cut

sub get_end_subheaders {

  my ( $self, $subheaders ) = @_;

  my @key = qw[ SEQSTART SEQEND ];

  my ( $init, $start, $end );
  for my $h ( @{ $subheaders } ) {

    next
      unless exists $h->{ $key[0] }
      && exists $h->{ $key[1] };

    my ( $k_start, $k_end ) = map { $h->{ $_ } } @key;

    unless ( $init ) {

      $end = $start = $h;
      $init++;
      next;
    }

    $start = $h if $start->{ $key[0] } >  $k_start;
    $end   = $h if $end->{ $key[1] }   <= $k_end;
  }

  return ( $start, $end );
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
  for my $sub ( @{ $subheaders } ) {

    my @keys = keys %{ $sub };

    unless ( $header_arr ) {
      for ( @keys ) {

        next if -1 == index lc $_, 'subarray';

        ( $array ) = ( $sub->{ $_ } =~ $array_re )
          and last;
      }
    }

    throw JSA::Error "Failed to find subarray type."
      unless $array;

    for ( @keys ) {

      $group->{ $array }{ $_ } ||= $sub->{ $_ };
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

    $header->{ join '_', $field, $col } = $header->{ $field };
    delete $header->{ $field };
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

  # Add 'subsysnr' field.
  my $parse = qr{^ (s[48]) [a-d] }xi;

  for my $i ( 0 .. scalar @file - 1 ) {

    next if $header->{'subsysnr'}[ $i ];

    ( $header->{'subsysnr'}[ $i ] ) = $file[ $i ] =~ $parse;
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

