package JSA::Command;

=head1 NAME

JSA::Command - Execute a shell command in the JSA environment

=head1 SYNOPSIS

    use JSA::Command;
    run_command(@args);

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

use JSA::Error qw/:try/;

use Exporter 'import';
our @EXPORT_OK = qw/run_command/;

=head1 FUNCTIONS

=over 4

=item B<run_command>

Run a shell command using the supplied arguments.

    run_command( $command, @args );

The command should include the full path to the command.
Throws C<JSA::Error::BadExec> with the contents of standard error
if the command fails.

Use C<JSA::Starlink::run_star_command> when running Starlink
commands.

In list context returns a reference to an array containing the
messages logged to standard out and a reference to an array containing
the messages sent to standard error.

    ($stdout, $stderr) = run_command( $command, @args );

The exit status is returned as the 3rd item in the list but that will
always be 0 if exceptions are enabled.

Control parameters can be passed in by using a reference to a hash
as the first argument.

    ($stdout, $stderr, $stat) = run_command( { nothrow => 1 }, $command, @args );

If exceptions are disabled the exit status can be non-zero.

Hash options are:

    nothrow => if true, disable exception throwing. This can be useful
               if an application embeds errors in standard out.

=cut

sub run_command {
    my %control = ( nothrow => 0 );
    if (ref($_[0]) eq 'HASH') {
        my $h = shift;
        %control = (%control, %$h);
    }

    # Now read the command
    my @args = @_;

    # Get some temp handles for stdout and stderr
    my ($out1, $err1, $out2, $err2) = tmpfh_out_err();

    my $exstat;
    my $conv = eval {
        # If the command is missing Proc::SafeExec dies
        Proc::SafeExec->new({ exec => \@args,
                              stdout => $out1,
                              stderr => $err1,
                            });
    };

    my (@stdout, @stderr);
    unless (defined $conv) {
        @stderr = split(/\n/, $@);
        $exstat = -1;
    }
    else {
        $conv->wait;
        $exstat = $conv->exit_status;

        # Now read back
        seek($out2, 0,0);
        @stdout = <$out2>;

        seek($err2, 0,0);
        @stderr = <$err2>;
    }

    # Sometimes we get a \r in the response so must clean it
    for (@stdout) {
        chomp;
        s/\r$//;
    }
    for (@stderr) {
        chomp;
        s/\r$//;
    }

    # see perlvar documentation
    my $exit_status = $exstat >> 8;
    my $signal = $exstat & 127;
    my $sigtext = '';
    if ($exstat == -1) {
        # Triggered from eval above
        $sigtext = "(via die) ";
        $exit_status = -1;
    }
    elsif ($signal) {
        $sigtext =  "(signal $signal) ";
    }
    elsif ($exit_status == 255) {
        $sigtext = "(via die) ";
    }

    throw JSA::Error::BadExec( "Error running command ".join(" ",@args)."\n".
        " - status = $exit_status $sigtext.".(@stderr ? " Errors:\n". join("\n",@stderr) : "")."\n" )
        if ($exit_status != 0 && !$control{nothrow});
    return (\@stdout,\@stderr, $exit_status);
}

=item B<tmpfh_out_err>

Return filehandles that can be used during command execution to
capture STDOUT and STDERR. Two filehandles are returned for STDOUT
and STDERR (the second pair are dupes of the first to allow C<Proc::SafeExec>
to close them in the child).

    ($out1, $err1, $out2, $err2) = tmpfh_out_err();

The first pair are pure filehandles, the second pair are File::Temp
objects. The first pair should be sent to C<Proc::SafeExec> and the
second pair should be retained for analysis after program execution.

=cut

sub tmpfh_out_err {
    my $out = File::Temp->new();
    my $err = File::Temp->new();
    open my $dup_out, "<&",$out or croak "Could not dupe temp out: $!";
    open my $dup_err, "<&",$err or croak "Could not dupe temp err: $!";
    return ($dup_out, $dup_err, $out, $err);
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
