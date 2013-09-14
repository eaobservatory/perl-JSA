package JSA::LogSetup;

=pod

=head1 NAME

JSA::LogSetup - Setup a default log configuration for Log::Log4perl

=head1 SYNOPSIS

  use JSA::LogSetup qw[ get_config logfile ];

Pass default configuration to L<Log::Log4perl> ...

  logfile( '/log/file/path' );
  Log::Log4perl->init( get_config() );

Note that changes made to config after this point via L<Log::Log4perl>
will not be reflected back when I<&get_config> is called again.

  $log = Log::Log4perl->get_logger( '' );

  # See Log::Log4perl for details.
  $log->error( 'Oops,', ' an error happened: ', $error_text );



=head1 DESCRIPTION

Creates a default log configuration suitable for L<Log::Log4perl>
consumption.  If a log file name is not specified, a file name is
generated with date and processor id in it.

Main purpose is to easily specify the log file name, and provide
wrapper to pass parameters to &I<Data::Dumper>::Dumper (see
L<Log::Log4perl>).

=head1 FUNCTIONS

Following functions can be imported into caller's namespace...

  get_config
  get_dumper
  hashref_to_dumper
  logfile
  make_logfile

Nothing is exported by default.

=cut

use strict; use warnings;

use Exporter qw[ import ];
our @EXPORT_OK =
  qw[ get_config
      logfile
      make_logfile
      get_dumper
      hashref_to_dumper
      set_default_format
      set_message_format
    ];

use Carp ();
use DateTime;
use File::Spec;
use File::Temp;
use Net::Domain ();

my $_log_dir = '/jac_logs/jsa';
$_log_dir = '/tmp' unless -d $_log_dir;

# In case no basename given for log file.
my $_log_basename = 'default.log';

my $_log_file_key = 'log4perl.appender.log.filename';
my $_log_fmt_key  = 'log4perl.appender.log.layout.ConversionPattern';

my %_config =
  ( 'log4perl.rootLogger' => 'DEBUG, log',

    'log4perl.appender.log'        => 'Log::Log4perl::Appender::File',
    $_log_file_key                 => undef,
    'log4perl.appender.log.mode'   => 'append',
    'log4perl.appender.log.layout' => 'PatternLayout',
  );
set_default_format();

=over 2

=item B<get_config>

Returns the default configuration as a hash reference, suitable for
direct consumption by L<Log::Log4perl>.

  Log::Log4perl->init( get_config() );

Some of the configuration defaults are ...

  level      - debug

  directory  - /jac_logs/jsa (or /tmp if missing)

  file       - /jac_logs/jsa/default.log.<host>.<user>.<yyyy:mm:dd>
               (appended, not overwritten)

  log format - %H %p %P %d{yyyyMMdd-hhmm:ss} %F %L %M%n  %m%n, or ...
                 %H : host
                 %p : log level
                 %P : process-id
                 %d : date-time
                 %F : file name
                 %L : line number
                 %M : sub name (if available)
                 %n : newline
                 %m : message

=cut

sub get_config { return { %_config }; }

=item B<set_default_format>

Sets the default, verbose format to
C<%H %p %P %d{yyyyMMdd-HHmm:ss} %F %L %M%n  %m%n>.

=cut

sub set_default_format {

  $_config{ $_log_fmt_key } =
    '%H %5p %6P %d{yyyyMMdd-HHmm:ss} %F %L %M%n  %m%n';

  return;
}

=item B<set_default_format>

Sets the default, short format to
C<%d{yyyyMMdd-hhmm:ss}%n  %m%n>.

=cut

sub set_message_format {

  $_config{ $_log_fmt_key } =
    '%d{yyyyMMdd-hhmm:ss}%n  %m%n';

  return;
}

=item B<get_dumper>

Returns code reference which would call &L<Data::Dumper>::Dumper,
passing it given list of elements.

  print get_dumper( [ 2, 3 ], [ 4, 6 ] )->();


Note the following variables are set before I<&Dumper> is called ...

    $Data::Dumper::Sortkeys
    $Data::Dumper::Indent
    $Data::Dumper::Deepcopy

See L<Data::Dumper> for details.

=cut

sub get_dumper {

  my ( @in ) = @_;

  return
    sub { require Data::Dumper;
          local $Data::Dumper::Sortkeys = 1;
          local $Data::Dumper::Indent = 1;
          local $Data::Dumper::Deepcopy = 1;
          return
            Data::Dumper::Dumper( @in );
        };
}

=item B<hashref_to_dumper>

Returns I<&get_dumper> output, and passes given arguments in a hash
reference.

  $log->debug( hashref_to_dumper( 'a' => 3, 'b' => 5 ) );

This is syntactic sugar to avoid calling I<&get_dumper> as ...

  $log->debug( get_dumper( { 'a' => 3, 'b' => 5 } ) );

=cut

sub hashref_to_dumper { return get_dumper( { @_ } ); }

=item B<logfile>

Returns the log file name if no arguments are specified.

  $file = logfile();

Otherwise, sets the log file to the given value.

  logfile( '/log/file/path' );

=cut

sub logfile {

  return $_config{ $_log_file_key }
    unless scalar @_;

  $_config{ $_log_file_key } = $_[0];
  return;
}

=item B<make_logfile>

When given basename contains the directory separator, C</>, it is
returned as is.

Given optional base name for log file, and optional parameters hash,
returns a file path based on current date (in UTC -10 time zone).

When possible, I<the log directory and log file are made group
writable>.

  # Sets log file to '/jac_logs/jsa/201201/20/enterdata' for date of
  # Jan 12 2012.
  logfile( make_logfile( 'enterdata' ) );

  # Keep log file elsewhere; '/tmp/201201/20/enterdata' in this case.
  logfile( make_logfile( 'enterdata',
                          'dir' => '/tmp'
                        )
          );

Optional hash key-values are ...

=over 2

=item I<add-dir> => truth value

Include parent directory name in the basename of the log file. C</> and
C<space> are replaced by C<=>.

=item I<add-pid> => truth value

Include process ID in the basename.

=item I<add-random> => truth value

Include a random string in the basename.

=item I<add-time> => truth value

Include current time in the basename, along with default inclusion of
current date.

=item I<dir> => (root) log directory

The directory in which to create a log file. (Default root log
directory is F</jac_logs/jsa>, or F</tmp> if missing.)

=item I<force-new> => truth value

Generate new name even if a name had already been generated based on
all other parameters which were the same as this time.

=item I<skip-date-dir> => truth value

Do not create sub directories in format C<yyyymm/dd> by default.

=back

=cut

{
  my %made;

  sub make_logfile {

    my ( $base, %opt ) = @_;

    # A directory has been already specified; nothing sane to do.
    $base && $base =~ m[/]
      and return $base;

    unless ( $base ) {

      $base = $_log_basename;

      Carp::carp( sprintf qq[No base name given to make log file path; using '%s' instead.\n],
                    $base
                );
    }

    my $dir = $opt{'dir'} || $_log_dir;

    my $track = join '#', $dir, $base,
                  map
                  { join '=', $_ , defined $opt{ $_ } ? $opt{ $_ } : 'undef' }
                  sort keys %opt;

    return $made{ $track }
      if $made{ $track } && ! $opt{'force-new'};

    my $date = DateTime->now( 'time_zone' => '-1000' );

    $base = join '.', $base, _make_base_parts( $date, $dir, %opt );

    my $path;
    if ( ! $opt{'skip-date-dir'} ) {

      $path = _make_per_day_logfile( $date, $dir, $base )
    }
    else {

      _make_group_writable( $dir );
      $path = File::Spec->catfile( $dir, $base )
    }
    _make_group_writable( $path );

    return $made{ $track } = $path;
  }
}

sub _make_base_parts {

  my ( $dt, $dir , %opt ) = @_;

  # Use effective or real user id.
  my $user = getpwuid( $> ) || getpwuid( $< ) || 'unknown-user';

  my $time =
    join '-',
      map { $dt->$_( ':' ) } ( 'ymd', $opt{'add-time'} ? 'hms' : () )
      ;

  return
    ( Net::Domain::hostname(),
      $user,
      $time,
      ( $opt{'add-dir'} && $dir ? _path_as_base( $dir )  : () ),
      ( $opt{'add-pid'}         ? $$               : () ),
      ( $opt{'add-random'}      ? _random_string() : () )
    );
}

sub _random_string {

  my ( $size ) = @_;

  $size ||= 8;

  my @source = ( 'a' .. 'z' , 'A' .. 'Z' , 0 .. 9 );
  my $out = '';
  $out .= $source[ rand @source ] while length $out < $size;

  return $out;
}

sub _path_as_base {

  my ( $in ) = @_;

  $in =~ s{[/ ]+}/=/g;
  return $in;
}

sub _make_per_day_logfile {

  my ( $date, $parent_dir, $basename ) = @_;

  require File::Path;
  # Get make_path() changes.
  import File::Path 2.08;

  my ( $y, $m, $d ) = map { sprintf '%02d', $date->$_() } ( 'year', 'month', 'day' );

  # Directory tree is something like /parent/201201/12 for date of Jan 12, 2012.
  my $dir = File::Spec->catfile( $parent_dir,
                                  join( '', $y, $m ),
                                  $d
                                );

  # umask is take into account for the final permissions.
  File::Path::make_path( $dir,
                          { 'verbose' => 0,
                            'mode'    => 0777,
                            'owner'   => 'jcmtarch',
                            'group'   => 'jcmt_data',
                          }
                        );
  # Regardless of umask, make directory at least user & group writable.
  _make_group_writable( $dir );

  return File::Spec->catfile( $dir, $basename );
}

sub _make_group_writable {

  my ( @file ) = @_;

  require Fcntl;
  import Fcntl ':mode';

  my $sys_log_re = _get_sys_log_re();

  # For file (0664).
  my $default = S_IRUSR() | S_IWUSR() | S_IRGRP() | S_IWGRP() | S_IROTH();

  for my $file ( @file ) {

    next unless $file =~ $sys_log_re;

    my $mode = $default;
    # Adjust for directory (0775).
    -d $file and $mode |= S_IXUSR() | S_IXGRP() | S_IXOTH();

    chmod $mode, $file;
  }

  return;
}

# Only care about system wide, multi user usable file paths.
sub _get_sys_log_re {

  my $re = qr[^/(?: jac_log | tmp\b )]x;
  return $re;
}


1;

__END__

=back

=head1  SEE ALSO

=over 2

=item *

Log::Log4perl

=item *

Data::Dumper

=back

=head1 AUTHOR

Anubhav <a.agarwal@jach.hawaii.edu>

=head1 COPYRIGHT

Copyright (C) 2010-2012 Science and Technology Facilities Council.
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

