package JSA::Logging;

=head1 NAME

JSA::Logging - Simple logging routines

=cut

use strict;
use warnings;

use parent qw/Exporter/;
our @EXPORT_OK = qw/log_message log_warning log_command/;

sub log_message {
    my $message = shift;
    chomp($message);
    print STDERR "$message\n";
}

sub log_warning {
    log_message($_[0]);
}

sub log_command {
    my ($cmd, $stdout, $stderr) = @_;
    if ($stdout) {
        for my $line (@$stdout) {
            log_message("$cmd: $line\n");
        }
    }
    if ($stderr) {
        for my $line (@$stderr) {
            log_message("$cmd ERROR: $line\n");
        }
    }
}

1;

__END__

=head1 AUTHORS

Extracted from jsawrapdr which was written by:

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>

=head1 COPYRIGHT

Copyright (C) 2008-2011 Science and Technology Facilities Council.
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
