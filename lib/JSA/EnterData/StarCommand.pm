package JSA::EnterData::StarCommand;

use strict; use warnings;

our $VERSION = '0.03';

=pod

=head1 NAME

JSA::EnterData::StarCommand - Functions related to Starlink command execution.

=head1 SYNOPSIS

  use JSA::EnterData::StarCommand;

  $starcom = JSA::EnterData::StarCommand->new();

  $starcom->try_command( 'command' => [qw/ command and arguments /] )
    or die "Could not run the command";

=head1 DESCRIPTION

This object oriented module has methods related to Starlink command execution.

=head2 METHODS

=over 2

=cut

use Log::Log4perl ;

use JSA::Error qw[ :try ];
use JSA::Starlink ();

our $Par_Get = '/star/bin/kappa/parget';

=pod

=item B<new> (constructor)

Returns a L<JSA::EnterData::StarCommand> object.

  $starcom = JSA::EnterData::StarCommand->new();

=cut

sub new {

  my ( $class, %arg ) = @_;

  my $obj = {};
  $obj = bless $obj, $class;
  $obj->verbose( 0 );
  return $obj;
}

=item B<verbose>

Sets the anount of verbosity when an argument is given. Else, returns the
current value.

=cut

sub verbose {

  my $self = shift @_;

  my $key = 'verbose';

  scalar @_ or return $self->{ $key };

  require Scalar::Util;
  $self->{ $key } = Scalar::Util::looks_like_number( $_[0] ) ? $_[0] : 0;
  return;
}

=pod

=item B<try_command>

Given a hash of I<command> key and Starlink command and arguments as array
reference value, returns a truth value to indicate that the execution was
successful (return code is 0).

It catches and logs L<JSA::Error::StarlinkCommand> errors; passes along
L<JSA::Error::BadExec> ones.

Optionally, provide I<return-code> key and a true value to receive and test the
return code yourself.

Optionally, provide I<error-text> key and a string value to print when the error
during command execution is L<JSA::Error::StarlinkCommand> type.

  try_command( 'command' => [qw/ command and arguments /] )
    or die "Could not run the command";

=cut

sub try_command
{
  my ( $self, %arg ) = @_;

  my $err_text = $arg{'error-text'};
  my @com      = @{ $arg{'command'} }
    or throw JSA::Error::BadArgs 'No Starlink command given to run.' ;

  $self->_command_name( $com[0] );

  my $cmd_run = join ' ', @com;

  my $log = Log::Log4perl->get_logger( '' );
  $log->debug( qq[# Command to be run: $cmd_run\n] );

  my ( $stdout, $stderr, $rc );
  try {

    ( $stdout, $stderr, $rc ) =
      JSA::Starlink::run_star_command( $com[0], @com[ 1 .. $#com ] );

    defined $stdout && scalar @{ $stdout } && $self->verbose()
      and print "# Standard output ...\n", join( "\n", @{ $stdout } ), "\n";

    if ( 1 < $self->verbose() ) {

      defined $stderr && scalar @{ $stdout }
        and print "# Standard error ...\n", join( "\n", @{ $stderr } ), "\n";

      defined $rc and print "# Exit code: ", $rc, "\n";
    }
  }
  catch JSA::Error::StarlinkCommand with {

    my ( $err ) = @_;

    $err_text = join "\n",
                  ( $err_text // () ),
                  qq[Error executing "$cmd_run" ...],
                  $err->text(),
                  ;

    my $fx = $self->verbose() ? 'error_warn' : 'error';
    $log->$fx( $err_text );

    $err->text() =~ /No such file or directory/
      and throw JSA::Error::FatalError $err;
  };
  # Allow JSA::Error::BadExec error to move up.

  $arg{'return-code'} and return $rc;

  # run_star_command() throws Error when $rc != 0.
  return defined $rc && $rc == 0;
}

=pod

=item B<get_value>

Given a list of fields related to the command run, returns a hash with fields
as keys and rleated values as values.

  %value = $starcom->get_value( 'A', 'B' );

=cut

{
  my %base;
  sub get_value {

    my ( $self, @field ) = @_;

    my $com = $self->_command_name()
      or throw JSA::Error::BadArgs( 'A command name has not been set. (Hint: has the command been run yet?)' );

    my $log = Log::Log4perl->get_logger( '' );
    unless ( scalar @field ) {

      my $fx = $self->verbose() ? 'logwarn' : 'warn';
      $log->$fx( "No fields given to fetch values for command $com ." );
      return;
    }

    require File::Basename;
    exists $base{ $com } or ( $base{ $com } ) = File::Basename::fileparse( $com );
    my $alt = $base{ $com };

    1 < $self->verbose()
      and print qq[# Command for which to fetch values: $alt\n];

    my %out;
    for my $f ( @field ) {

      2 < $self->verbose() and print qq[# Fetching value for $f\n];

      $out{ $f } = qx{ $Par_Get $f $alt };
      for ( $out{ $f } ) {

        defined $_ or next;
        s/^\s+//;
        s/\s+$//;

        $self->verbose() and printf "# %s : %s\n", $f, $out{ $f };
      }
    }
    return %out;
  }
}


=pod

=back

=head2 INTERNAL METHODS

=over 2

=item B<_command_name>

Sets command name for internal use when an argument is given. Else, returns the
command name.

=cut

sub _command_name {

  my $self = shift @_;

  my $key = 'command-name';

  scalar @_ or return $self->{ $key };

  $self->{ $key } = $_[0];
  return;
}



1;

__END__

=pod

=back

=head1 COPYRIGHT, LICENSE

Copyright (C) 2013 Science and Technology Facilities Council.
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


