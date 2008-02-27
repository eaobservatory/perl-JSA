package JSA::Command;

=head1 NAME

JSA::Command - Execute a shell command in the JSA environment

=head1 SYNOPSIS

  use JSA::Command;
  run_command( @args );

=head1 DESCRIPTION

This module provides an interface for running external shell commands.
Standard output and standard error are dealt with correctly for the JSA
environment.

=cut

use strict;
use Carp;
use warnings;
use File::Spec;
use warnings::register;

use Proc::SafeExec;

use JSA::Error qw/ :try /;

use Exporter 'import';
our @EXPORT_OK = qw/ 
                     run_command
                   /;

=head1 FUNCTIONS

=over 4

=item B<run_command>

Run a shell command using the supplied arguments.

  run_command( $command, @args );

The command should include the full path to the command.
Throws C<JSA::Error::BadExec> if the command fails.

Use C<JSA::Starlink::run_star_command> when running Starlink
commands.

=cut

sub run_command {
  my @args = @_;

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

