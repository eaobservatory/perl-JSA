package JSA::Starlink;

=head1 NAME

JSA::Starlink - Helper functions to support the Starlink environment.

=head1 SYNOPSIS

  use JSA::Starlink;
  check__star_env( "CONVERT", "ndf2fits" );

=head1 DESCRIPTION

This module provides helper function that are useful when working
in the Starlink environment.

=cut

use strict;
use Carp;
use warnings;
use File::Spec;
use warnings::register;

use Proc::SafeExec;

use JSA::Error qw/ :try /;

use Exporter 'import';
our @EXPORT_OK = qw/ check_star_env
                     run_star_command
                   /;

=head1 FUNCTIONS

=over 4

=item B<check_star_env>

Check that the Starlink environment is okay. Without arguments
simply checks that $STARLINK_DIR is set and pointing to a
directory.

  check_star_env();

The first argument (optional) refers to the name of an application
whose environment variable should be tested.

  check_star_env( $appname, $command );

The second optional argument refers to a command within that application
which should exist in $appname_DIR directory.

Throws a C<JSA::Error::BadEnv> if there is something wrong with
the environment.

=cut

sub check_star_env {
  my $appname = shift;
  my $command = shift;

  throw JSA::Error::BadEnv( "Do not know where the Starlink software is installed. Please set \$STARLINK_DIR.") 
    unless (exists $ENV{STARLINK_DIR} && -d $ENV{STARLINK_DIR});

  if (defined $appname) {
    my $env = $appname;
    $env .= "_DIR" unless $appname =~ /_DIR$/;

    throw JSA::Error::BadEnv("$env environment variable is either not set or the directory does not exist")
      unless (exists $ENV{$env} && -d $ENV{$env});

    # check for the command
    if (defined $command) {

      throw JSA::Error::BadEnv("Command '$command' does not seem to exist in $ENV{$env} directory")
        unless -e File::Spec->catfile($ENV{$env}, $command );

    }

  }

}

=item B<run_star_command>

Run a Starlink command using the supplied arguments.

  run_star_command( $command, @args );

The command should include the full path to the command.
Throws C<JSA::Error::BadExec> if the command fails.

=cut

sub run_star_command {
  my @args = @_;

  # Make sure we get an exit status
  local $ENV{ADAM_EXIT} = 1;

  my $conv = Proc::SafeExec->new( { exec => \@args,
                                    stdout => "new"
                                  } );

  $conv->wait;

  my $exstat = $conv->exit_status >> 8;
  throw JSA::Error::BadExec( "Error running command $args[0] - status = $exstat" )
    if $exstat != 0;
  return 1;
}

=back

=head1 AUTHORS

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>,

=head1 COPYRIGHT

Copyright (C) 2008 Science and Technology Facilities Council.
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

