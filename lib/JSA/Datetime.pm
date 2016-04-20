package JSA::Datetime;

=pod

=head1 NAME

JSA::Datetime - Parse a date-time value, returns L<DateTime> object.

=head1 SYNOPSIS

    use JSA::DateTime qw/make_datetime/;

    $dt = make_datetime('2010-03-24T03:00:00');

    $syb_dt = make_datetime('Mar  3 2010');

    print 'ISO date > Sybase date' if $dt > $syb_dt;

=head1 DESCRIPTION

Purpose is to convert date or datetime strings into same format of
string or object so that they can be compared.  As a side effect, a
given string is also verified being a valid date or datetime value.

Currently it uses L<DateTime> module and returns objects of the same.

=head1 FUNCTIONS

Nothing is exported by default.  Following functions can be imported
...

    make_datetime
    make_limited_datetime
    parse_iso8601_datetime

=over 2

=cut

use strict;
use warnings;

use Exporter qw/import/;
our @EXPORT_OK = qw/
    make_datetime
    make_limited_datetime
    parse_iso8601_datetime
/;

use DateTime;

use JSA::Datetime::Sybase qw/parse_syb_datetime/;

=item B<make_datetime>

Parses, verifies a given date; if missing, current UTC date is used.
Returns a L<DateTime> object.

    $dt = make_datetime(20100324);

ISO8601 and default Sybase display date & datetime strings are
accepted.

=cut

sub make_datetime {
    my ($time, $tz) = @_;

    my %tz = ('time_zone' => $tz || 0);

    return parse_syb_datetime($time, $tz{'time_zone'})
        if $time
        && $time =~ JSA::Datetime::Sybase::_syb_datetime_regex();

    my $now = DateTime->now(%tz);

    return $now
        unless defined $time;

    # Pass optional base date to take care of given time zone.
    return parse_iso8601_datetime( $time, $now );
}

=item B<parse_iso8601_datetime>

Parses, verifies a given ISO8601 date or datetime string.  Returns a
L<DateTime> object.  It takes an optional base L<DateTime> object (see
L<DateTime::Format::ISO8601> for details).

    $dt = parse_iso8601_datetime('2010-03-24');

=cut

sub parse_iso8601_datetime {
    my ($time, $base_dt) = @_;

    require DateTime::Format::ISO8601;

    my $dt_format = DateTime::Format::ISO8601->new(
        $base_dt ? ('base_datetime' => $base_dt) : ());

    return  $dt_format->parse_datetime($time);
}

=item B<make_limited_datetime>

Given an ISO8601 date or datetime string, returns yesterday's date, as
L<DateTime> object, if current time is less than 01:00:00.  Else,
returns current date with 01:00:00 as the time portion (also as
L<DateTime> object).

    $lim_dt = make_limited_datetime($date_string);

=back

=cut

sub make_limited_datetime {
    my $now = make_datetime();

    my $gap_hr = 1;
    my $limit = _make_from_parts('hour' => $gap_hr,
                                 map {$_ => $now->$_} qw/year month day/);

    if ($now < $limit) {
        require DateTime::Duration;

        my $day = DateTime::Duration->new('days' => 1);
        my $past = $now - $day;
        $past->$_(0) foreach qw/set_hour set_minute set_second/;

        return $past;
    }

    return $limit;
}


=head1 INTERNAL FUNCTIONS

=over 2

=item B<_get_parts>

Returns a hash of datetime parts given a L<DateTime> object.  This
hash can be used directly to create a L<DateTime> object.

    %parts = _get_parts($dt);

=cut

sub _get_parts {
    my ($dt) = @_;

    return ('time_zone' => $dt->time_zone_long_name(),
            map {$_ => $dt->$_} qw/year month day hour minute second/);
}

=item B<_make_from_parts>

Returns a L<DateTime> object given a hash of datetime parts.

    $dt = _make_from_parts(%parts);

Hour, minute, second, and time zone are set to 0 if corresponding
values are missing from the hash.

=back

=cut

sub _make_from_parts {
    my (%parts) = @_;

    foreach (qw/hour minute second time_zone/) {
        $parts{$_} = 0 unless defined $parts{$_};
    }

    return DateTime->new(%parts);
}


1;


__END__

=pod

=head1 SEE ALSO

=over 2

=item *

JSA::Datetime::Sybase for Sybase date, datetime, or time parsing

=item *

DateTime

=item *

DateTime::Duration

=item *

DateTime::Format::ISO8601


=back

=head1 AUTHORS

=over 2

=item *

Anubhav E<lt>a.agarwal@jach.hawaii.eduE<gt>

=back

=head1 COPYRIGHT

Copyright (C) 2010, Science and Technology Facilities Council.
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
