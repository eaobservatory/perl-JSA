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

  $log = Log::Log4perl->get_logger();

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

use Carp qw[ croak carp ];
use Exporter qw[ import ];
use File::Spec;

our @EXPORT_OK =
  qw[ get_config
      logfile
      make_logfile
      get_dumper
      hashref_to_dumper
    ];

my $_log_dir = '/jac_logs/jsa';
$_log_dir = '/tmp' unless -d $_log_dir;

my $_log_file_key = 'log4perl.appender.log.filename';

my %_config =
  ( 'log4perl.rootLogger' => 'DEBUG, log',

    'log4perl.appender.log'        => 'Log::Log4perl::Appender::File',
    $_log_file_key                 => make_logfile( 'log' ),
    'log4perl.appender.log.mode'   => 'append',
    'log4perl.appender.log.layout' => 'PatternLayout',

    'log4perl.appender.log.layout.ConversionPattern' =>
      # host log-level process-id time
      # - file name line-number (sub, if available)
      # - message\n
      '%H %p %P %d{yyyyMMdd-hhmm:ss} %F %L %M # %m%n'
  );

=over 2

=item B<get_config>

Returns the default configuration as a hash reference, suitable for
direct consumption by L<Log::Log4perl>.

  Log::Log4perl->init( get_config() );

Some of the configuration defaults are ...

  level     - debug

  directory - /jac_logs/jsa

  file      - /jac_logs/jsa/default.<yyyymmdd>.<process id>
              (appeneded, not overwritten)

  format    - %H %p %P %d{yyyyMMdd-hhmm:ss} %F %L %M # %m%n, or ...
                %H : host
                %p : log level
                %P : process-id
                %d : date-time
                %F : file name
                %L : line number
                %M : sub name (if available)
                #
                %m : message
                %n : newline

=cut

sub get_config { return { %_config }; }

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

Returns a file path with date and process id appended to given file
name prefix (devoid of '/') and optional log directory. (Default log
directory is used otherwise.)

  # Sets log file to '/jac_logs/jsa/enterdata.20100612.2345',
  # (when current date is Jun 12, 2010 and process id is 2345).
  logfile( make_logfile( 'enterdata' ) );

  # Sets log file to '/tmp/enterdata.20100612.2345',
  logfile( make_logfile( 'enterdata', '/tmp' ) );

=cut

sub make_logfile {

  my ( $prefix, $dir ) = @_;

  croak 'No prefix given to make a file name.'
    unless $prefix;

  require DateTime;
  return
    File::Spec->catfile( $dir || $_log_dir,
                          join '.', $prefix, DateTime->now( 'time_zone' => 0)->ymd( '' );
                        );
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

Copyright (C) 2010 Science and Technology Facilities Council.
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

