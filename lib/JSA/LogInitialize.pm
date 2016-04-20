package JSA::LogInitialize;

=pod

=head1 NAME

JSA::LogInitialize - Initialize a Log::Log4perl object

=head1 SYNOPSIS

    use JSA::LogInitialize qw/init_logger/;

    init_logger('/log/file/path');


=head1 DESCRIPTION

Creates a C<Log::Log4perl> object with default log configuration
suitable for L<Log::Log4perl> consumption. If a log file name is not
specified, a file name is generated via C<JSA::LogSetup>.

Main goal is to be able to change log file path based on some
criterion; e.g. every 24 hours with date being part of file name.

=head1 FUNCTIONS

Following functions can be imported into caller's namespace...

    init_logger

Nothing is exported by default.

=cut

use strict; use warnings;

use Exporter qw/import/;
our @EXPORT_OK = qw/init_logger/;

use Log::Log4perl;
use Log::Log4perl::Level 'L4P';

use JSA::LogSetup ();

{
    my %done;
    sub init_logger {
        my ($name, %opt) = @_;

        my $path = JSA::LogSetup::make_logfile($name, %opt);

        return $path if exists $done{$path};

        JSA::LogSetup::logfile($path);
        Log::Log4perl->init(JSA::LogSetup::get_config());

        my $log = Log::Log4perl->get_logger('');
        $log->level($opt{'level'} || $L4P::WARN);

        undef $done{$path};
        return $path;
    }
}


1;

__END__

=over

=item B<init_logger>

Returns the log file name given a suitable name (see
C<&JSA::LogSetup::make_logfile>), initializes the C<Log::Log4perl>
object (see the module pod on how to use the object).

    printf "Log file is %s\n", init_logger('basename-or-path');

It also takes optional arguments to specify log level (see
C<Log::Log4perl::Level>), and to pass the rest to
C<&JSA::LogSetup::make_logfile>. If no log level is specified,
C<$Log::Log4perl::WARN> is used.

=cut

=back

=head1  SEE ALSO

=over 2

=item *

JSA::LogSetup

=item *

Lg::Log4perl

=item *

Log::Log4perl::Level

=back

=head1 COPYRIGHT

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
