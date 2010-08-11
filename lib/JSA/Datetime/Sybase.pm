package JSA::Datetime::Sybase;

=pod

=head1 NAME

JSA::Datetime::Sybase - Parse a date-time value, returns L<DateTime> object.

=head1 SYNOPSIS

  use JSA::DateTime::Sybase qw[ parse_syb_datetime ];

  $dt = parse_syb_datetime( 'Mar  3 2010' );

=head1 DESCRIPTION

Purpose is to convert Sybase default display date, datetime, or time
("Sybase date,  datetime, or time" henceforth) strings into same
format so that they can be compared.  As a side effect, a given string
is also verified being a valid date or datetime value.

Currently it uses L<DateTime> module and returns objects of the same.

=head1 FUNCTIONS

Nothing is exported by default.  Following functions can be imported
...

  parse_syb_datetime
  get_syb_format

=over 2

=cut

use strict;
use warnings;

use Exporter qw[ import ];
our @EXPORT_OK =
  qw[ parse_syb_datetime
      get_syb_format
    ];

=item B<parse_syb_datetime>

Parses, verifies a given Sybase date, datetime, or time string.
Returns a L<DateTime> object.  It takes an optional time zone value
(if missing, UTC is used).

  $dt = parse_syb_datetime( 'Mar  3 2010' );

=cut

sub parse_syb_datetime {

  my ( $time, $tz ) = @_;

  require DateTime::Format::Strptime;

  my $dt_format =
    DateTime::Format::Strptime
    ->new( 'pattern'    => get_syb_format( $time ),
            'time_zone' => $tz || 0,
            'on_error'  => 'croak'
          );

  return $dt_format->parse_datetime( $time );
}

=item B<get_syb_format>

Return a date, datetime, or time format (see C<strptime(3)> and
L<DateTime::Format::Strptime>) given a Sybase date, datetime, or time
string.

  $date_format = get_syb_format( 'Mar  3 2010' );
  $time_format = get_syb_format( ' 3:12AM' );

=back

=cut


sub get_syb_format {

  my ( $time ) = @_;

  my $time_re = _syb_time_regex();
  return _syb_time_format() if $time =~ /^$time_re$/;

  my $date_re = _syb_date_regex();
  return _syb_date_format() if $time =~ /^$date_re$/;

  if ( $time =~ _syb_datetime_regex() ) {

    my $milli = $1;
    return
      $milli
      ? _syb_datetime_ms_format()
      : _syb_datetime_format()
      ;
  }

  return;
}

=head1 INTERNAL FUNCTIONS

=over 2

=item B<_syb_date_regex>

Returns regex to match Sybase date without any anchors or boundary
match.

=cut

sub _syb_date_regex {

  return
    qr{ # Month
        [A-Z][a-z]{2}
        # Day
        \s (?: \s | \d )\d
        # Year
        \s \d{4}
      }x;
}

=item B<_syb_time_regex>

Returns regex to match Sybase time value without any anchors or
boundary match.  The regex has one capture group for optional
milliseconds.

=cut

sub _syb_time_regex {

  return
    qr{ # Hour
        (?: \s | \d )\d
        # Minute
        : \d{2}
        # To indicate if time has milli seconds.
        (?: : (\d{3}) )?
        [AP]M
      }x;
}

=item B<_syb_datetime_regex>

Returns regex to match Sybase datetime value anchored at end points.
The regex has one capture group for optional milliseconds.

=cut

sub _syb_datetime_regex {

  my $date = _syb_date_regex();
  my $time = _syb_time_regex();

  # An example of default value of a DATETIME type column is
  # "Aug  3 2010  3:00AM".
  return qr{ ^ $date \s $time $ }x;
}

=item B<_syb_date_format>

Returns format for Sybase date value.

=cut

sub _syb_date_format { return q[%b %e %Y]; }

=item B<_syb_time_format>

Returns format for Sybase time value without millisecond.

=cut

sub _syb_time_format { return q[%l:%M%p]; }

=item B<_syb_time_ms_format>

Returns format for Sybase time value with millisecond.

=cut

sub _syb_time_ms_format { return q[%l:%M:%03d%p]; }

=item B<_syb_datetime_format>

Returns format for Sybase datetime value without millisecond.

=cut

sub _syb_datetime_format {

  # Default example is "Aug  3 2010  3:00AM".
  return join ' ', _syb_date_format(), _syb_time_format();
}


=item B<_syb_datetime_ms_format>

Returns format for Sybase datetime value with millisecond.

=cut

sub _syb_datetime_ms_format {

  # Use %03d as there is no millisecond option in *::Format::Strptime or in
  # strftime(3) manual page.
  return join ' ', _syb_date_format(), _syb_time_ms_format();
}


1;


__END__

=pod

=back

=head1 SEE ALSO

=over 2

=item *

JSA::Datetime

=item *

DateTime

=item *

DateTime::Format::Strptime

=item *

strptime(3)

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

