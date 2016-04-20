package JSA::EnterData::DAS;

use strict;
use warnings;

use base 'JSA::EnterData::ACSIS';

=head1 NAME

JSA::EnterData::DAS - DAS specific methods.

=head1 SYNOPSIS

    # Create new object, with specific header dictionary.
    my $inst = new JSA::EnterData::DAS;

    my $name = $inst->name();

    my @cmd = $inst->get_bound_check_command;
    system(@cmd) == 0
        or die "Problem with running bound check command for $name.";

    # Use table in a SQL later.
    my $table = $inst->table();


=head1 DESCRIPTION

JAS::EnterData::DAS is a object oriented module, having instrument specific
methods in order to be called from L<JSA::EnterData>.

It inherits from L<JSA::EnterData::ACSIS>.

=head2 METHODS

=over 2

=cut

=item B<new>

Constructor, returns an I<JSA::EnterData::DAS> object.

    $enter = new JSA::EnterData::DAS();

Currently, no extra arguments are handled.

=cut

sub new {
    my ($class, %args) = @_;

    my $obj = $class->SUPER::new(%args);
    return bless $obj, $class;
}

=item B<name>

Returns the name of the backend involved.

    $name = $inst->name();

=cut

sub name {
    return 'DAS';
}


=item B<raw_basename_regex>

Returns the regex to match base file name, with array, date and run
number captured ...

    qr{ [ah]
        (\d{8})
        _
        (\d{5})
        _\d{2}_\d{4}[.]sdf
      }x;

    $re = JSA::EnterData::DAS->raw_basename_regex();

=cut

sub raw_basename_regex {
    return
        qr{ \b
            [ah]
            ([0-9]{8})       # date,
            _
            ([0-9]{5})       # run number,
            _[0-9]{2}        # subsystem.
            _[0-9]{4}[.]sdf
            \b
          }x;
}


1;

=pod

=back

Copyright (C) 2013, Science and Technology Facilities Council.
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
