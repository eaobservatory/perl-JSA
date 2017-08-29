package JSA::EnterData::StarCommand;

=head1 NAME

JSA::EnterData::StarCommand - Functions related to Starlink command execution.

=head1 SYNOPSIS

    use JSA::EnterData::StarCommand qw/try_star_command/;


    $values = try_star_command(
            command => [qw/command and arguments/],
            values => [qw/.../]);

=head1 DESCRIPTION

This module has functions related to Starlink command execution.

=head2 FUNCTIONS

=over 2

=cut

use strict;
use warnings;

our $VERSION = '0.03';

use base qw/Exporter/;

use File::Basename;
use File::Temp;
use Log::Log4perl;

use JSA::Error qw/:try/;
use JSA::Starlink qw/run_star_command/;

our @EXPORT_OK = qw/try_star_command/;
our $PARGET = '/star/bin/kappa/parget';

=item B<try_star_command>

Given a hash of I<command> key and Starlink command and arguments as array
reference value, returns a reference to a hash containing the requested
I<values> retrieved from the command's output.  Values which could not be
retrieved are given a value of undef.

It catches and logs L<JSA::Error::StarlinkCommand> errors,
returning undef.  It passes along L<JSA::Error::BadExec> errors.

If no values are requested and an error did not occur, returns 1.

    $values = try_star_command(
        command => [qw/command and arguments/],
        values => [qw/values to fetch/])

=cut

sub try_star_command {
    my %arg = @_;

    my @com = @{$arg{'command'}}
        or throw JSA::Error::BadArgs 'No Starlink command given to run.';

    my $cmd_run = join ' ', @com;
    my $com_name = shift @com;

    my $log = Log::Log4perl->get_logger('');
    $log->debug("# Command to be run: $cmd_run");

    # Set temporary ADAM directory to ensure we retrieve parameters from the
    # correct instance of the command.
    my $adam_dir = File::Temp->newdir();
    local $ENV{'ADAM_USER'} = "$adam_dir";
    $log->debug("# Using temporary ADAM directory: $adam_dir");

    my $rc;
    try {
        my ($stdout, $stderr);
        ($stdout, $stderr, $rc) = run_star_command($com_name, @com);

        $log->debug("# Standard output ...\n", join("\n", @{$stdout}))
            if defined $stdout && scalar @{$stdout};

        $log->debug("# Standard error ...\n", join("\n", @{$stderr}))
            if defined $stderr && scalar @{$stdout};

        $log->debug("# Exit code: ", $rc, "\n")
            if defined $rc;
    }
    catch JSA::Error::StarlinkCommand with {
        my ($err) = @_;

        $log->error_warn(join "\n",
            "Error executing \"$cmd_run\" ...",
            $err->text());

        throw JSA::Error::FatalError $err
            if $err->text() =~ /No such file or directory/;
    };

    # Allow JSA::Error::BadExec error to move up.

    # run_star_command() throws Error when $rc != 0.
    return undef unless defined $rc && $rc == 0;

    return 1 unless exists $arg{'values'};

    my @values = @{$arg{'values'}};

    my $base = File::Basename::fileparse($com_name);

    $log->debug("# Command for which to fetch values: $base");

    my %out;

    local $ENV{'ADAM_EXIT'} = '1';

    for my $f (@values) {
        $log->debug("# Fetching value for $f");

        my $v = `$PARGET $f $base`;

        if ($?) {
            $log->warn("# Could not fetch value for $f");
            $out{$f} = undef;
            next;
        }

        $v =~ s/^\s+//;
        $v =~ s/\s+$//;

        $log->debug(sprintf("# %s : %s", $f, $v));

        $out{$f} = $v;
    }

    return \%out;
}

1;

__END__

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
