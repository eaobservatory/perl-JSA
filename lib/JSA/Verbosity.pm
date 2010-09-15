package JSA::Verbosity;

=pod

=head1 NAME

JSA::Verbose - Sets verbosity level.

=head1 SYNOPSIS

  $noise = JSA::Verbosity->new( 1 );

  $noise->verbose() and warn "Verbosity is on";

  # Does not print anything as level 2 > 1.
  $noise->make_noise( 2, 'zero' );

  # Prints "Verbosity ...".
  $noise->make_noise( 1, 'Verbosity is on' );

=head1 DESCRIPTION

This module provides one place to use & set verbosity level, and print
messages based on certain threshold.

=cut

use strict; use warnings;

use Carp qw[ carp croak ];


=item B<new>

Returns a L<JSA::Verbosity> object, given an optional verbosity level
as an integer and optional turth value to force creation of new
object.  If force parameter is not given, then an already existing
instance is returned.

  # new() is called first time.
  $noise = JSA::Verbosity->new( 1 );

  # $noise2 is same as above $noise (use verbosity() to change level
  # at this point).
  $noise2 = JSA::Verbosity->new( 2 );

  # $noise3 is different than all of the above.
  $noise2 = JSA::Verbosity->new( 2, 'force' );

=cut

{
  my $_obj;

  sub new {

    my ( $class, $level, $force ) = @_;

    return $_obj
      if ! $force
      && defined $_obj && ref $_obj;

    $level =
      defined $level
      ? 0 + $level
      : 0
      ;

    return $_obj = bless \$level, $class;
  }
}

=item B<verbose>

Returns the current verbosity level.

  $noise->verbose() and warn "Verbose mode set";

=cut

sub verbose {

  my $self = shift @_;

  return ${ $self };
}

=item B<make_noise>

Prints nothing if level is zero.  Else, prints given list of strings
on standard error if given verbose level is less than or eqault to the
set non-zero verbose level.

  $noise->verbose( 1 );

  # Prints 'failed'.
  $noise->make_noise( 1, 'failed' );

  # Does not print anything.
  $noise->make_noise( 2, 'oops' );

  # Turn off all make_noise() output here on.
  $noise->verbose( 0 );

=cut

sub make_noise {

  my ( $self, $min, @msg ) = @_;

  return
    unless scalar @msg
    && $self->verbose();

  ( $min || 0 ) <= $self->verbose()
    and print STDERR @msg;

  return;
}


1;

__END__

=pod

=back

=head1 AUTHOR

Anubhav <a.agarwal@jach.hawaii.edu>

=head1 COPYRIGHT

Copyright (C) 2010 Science and Technology Facilities Council.
All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 2 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful,but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place,Suite 330, Boston, MA  02111-1307, USA

=cut

