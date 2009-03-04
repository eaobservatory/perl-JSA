package JSA::EnterData::SCUBA2;

use strict;
use warnings;

=head1 NAME

JSA::EnterData::SCUBA2 - SCUBA2 specific methods.

=head1 SYNOPSIS

  # Create new object, with specific header dictionary.
  my $inst = JSA::EnterData::SCUBA2->new

  my $name = $inst->name;

  my @cmd = $inst->get_bound_check_command;
  system( @cmd ) == 0
    or die "Problem with running bound check command for $name.";

  # Use table in a SQL later.
  my $table = $inst->table;


=head1 DESCRIPTION

JAS::EnterData::SCUBA2 is a object oriented module, having instrument specific
methods in order to be called from L<JSA::EnterData>.

=head2 METHODS

=over 2

=cut

=item B<new>

Constructor, returns an I<JSA::EnterData::SCUBA2> object.

  $enter = JSA::EnterData::SCUBA2->new;

Currently, no extra arguments are handled.

=cut

sub new {

  my ( $class ) = @_;

  my $obj = bless { }, $class;
  return $obj;
}

=item B<calc_freq>

Noop currently.

=cut

sub calc_freq {

  my ( $self ) = @_;

  return;
}


=item B<fill_headers>

Fills in the headers for C<SCUBA2> database table, given a headers
hash reference and an L<OMP::Info::Obs> object.

  $enter->fill_headers( \%header, $obs );

=cut

sub fill_headers {

  my ( $self, $header, $obs ) = @_;

  my $obsid = $obs->obsid;
  my @subscans = $obs->simple_filename;
  $header->{'max_subscan'} = scalar @subscans;

  return;
}


=item B<get_bound_check_command>

Returns a list of command and its argument to be executed to
check/find the bounds.

  @cmd = $inst->get_bound_check_command;

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
      'reset'
    );
}


=item B<name>

Returns the name of the instrument involved.

  $name = $inst->name;

=cut

sub name { return 'SCUBA-2' ; }


=item B<table>

Returns the database table related to the instrument.

  $table = $inst->table;

=cut

sub table { return 'SCUBA2'; }


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

