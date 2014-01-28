package JSA::WriteList;

use strict; use warnings;

our $VERSION = '0.01';

use Exporter 'import';
our @EXPORT_OK = qw[ write_list clear_list ];

use Carp       ();

use JSA::Error qw[ :try ];

=pod

=head1 NAME

JSA::WriteList - Functions to write a list to a temporary file.

=head1 SYNOPSIS

  use JSA::WriteList qw( write_list clear_list );

  $file = write_list( $save, [ '/tmp/one' , '/tmp/two' ] );
  # . . .
  clear_list();

=head1 DESCRIPTION

This funcational module has methods related writing a file list to a
file.

=head2 FUNCTIONS

=over 2

=item B<write_list>

Writes a given array reference of files to a given file; returns a
true value on success.  On error, throws L<JSA::Error>. If C<close()>
fails, then it C<croak>s (see L<Carp>).

  $ok = write_list( $path, [ '/tmp/one' , '/tmp/two' ] );

=item B<clear_list>

Removes the file gievn earlier. On success, returns a true value; on
error, prints a warning & returns nothing.

  clear_list();

=cut

{
  my ( $path );
  sub write_list {

    my ( $save, $files ) = @_;

    undef $path;

    open my $fh, '>', $save
      or throw JSA::Error::FatalError( qq[Cannot write to $save: $!] );

    my $err;
    for my $i ( 0 .. $#{ $files } ) {

      unless ( print $fh $files->[ $i ] . "\n" ) {

        $err = sprintf "%s (lines written so far: %d)" , $! , $i;
        last;
      }
    }

    close( $fh )
      or Carp::croak( qq[Could not close $path: $!] );

    $err
      and throw JSA::Error::FatalError( qq[Error writing to $save: $err] );

    $path = $save;
    return 1;
  }

  sub clear_list {

    if ( defined $path && -e $path && ! unlink( $path ) ) {

      Carp::carp( qq[Could not remove $path: $!] );
      return;
    }

    return 1;
  }
}


1;

=pod

=back

=head1 COPYRIGHT, LICENSE

Copyright (C) 2014 Science and Technology Facilities Council.
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

