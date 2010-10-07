package JSA::DB::TableTransfer;

=pod

=head1 NAME

JSA::DB::TableTransfer - Check file replication, transfer files to CADC

=head1 SYNOPSIS

Make an object ...

  $xfer = JSA::DB::TableTransfer->new( 'dbhandle' => $dbh,
                                      );

Set replicated state for a file ...

  $xfer->set_replicated( [ 'a20100612_00005_01_0001.sdf',
                            'a20100612_00006_01_0001.sdf'
                          ]
                      );

Find replicated files in CADC database ...

  $xfer->get_replicated_files( 20100612 );

=head1 DESCRIPTION

This package manipulates the database table to track progress of raw file data
ingestion and replication to CADC.

The file ingestion process adds a row on successful ingestion; file copy to CADC
& table replication tracking processes update the state. Disk cleaning process
should delete the rows (to be implemented).

=cut

use strict; use warnings;

use File::Spec;
use DateTime;
use DateTime::Duration;
use Pod::Usage;
use Getopt::Long qw[ :config gnu_compat no_ignore_case no_debug ];
use List::Util qw[ min sum ];
use List::MoreUtils qw[ any ];
use Log::Log4perl;

use JAC::Setup qw[ omp ];

use JSA::Error qw[ :try ];
use OMP::Config;

$OMP::Config::DEBUG = 0;

my $_state_table = 'transfer';
my $_state_descr_table = 'transfer_state';

=head1 METHODS

All the C<get_*_files> methods will be changed to not require partial file name
in future.

The state types are ...

  copied
  delete
  error
  found
  ignored
  ingested
  replicated
  transferred
  simulation
  final

=over 2

=item B<new> constructor

Make a C<JSA::DB::TableTransfer> object.  It takes a hash of parameters ...

  dbhandle     - JSA::DB object, which has succesfully connected to database;
  transactions - (optional) truth value, used when changing tables;

  $xfer =
    JSA::DB::TableTransfer->new( 'dbhandle' => $dbh,
                                );

Throws L<JSA::Error::BadArgs> error when database handle is invalid object.

=cut

sub new {

  my ( $class, %arg ) = @_;

  my $dbh = $arg{'dbhandle'};
  throw JSA::Error::BadArgs "Database handle object is invalid."
    unless defined $dbh
    && ref $dbh;

  my $obj = { };

  for (qw[ dbhandle transactions ] ) {

    next unless exists $arg{ $_ };

    $obj->{ $_ } = $arg{ $_ };
  }

  $obj = bless $obj, $class;

  return $obj;
}

BEGIN {
  our %_state =
    (
      #  File found on JAC disk (table has full path).
      'found' => 'f',

      #  Error causing file.
      'error' => 'e',

      # File is a deletion candidate.
      'delete' => 'd',

      # File has been deleted.
      'deleted' => 'D',

      'ignored' => 'x',

      'simulation' => 's',

      #  File has been locally ingested.
      'ingested' => 'i',

      #  File appeared in CADC database.
      'replicated' => 'r',

      #  File has been put in CADC transfer directory.
      'copied' => 'c',

      #  File present in CADC directory.
      'transferred' => 't',

      'final' => 'z',
    );

=item B<get_copied_files>

Return a array reference of files with C<copied> state, given a partial file
name.

  $files = $xfer->get_copied_files( 20100612 );

=item B<add_copied>

Add state of C<copied> of given array reference of files (base names).

  $xfer->add_copied( [ @files ] );

=item B<set_copied>

Set state to C<copied> of given array reference of files (base names).

  $xfer->set_copied( [ @files ] );

=item B<get_error_files>

Return a array reference of files with C<error> state, given a partial file
name.

  $files = $xfer->get_error_files( 20100612 );

=item B<add_error>

Add state of C<error> of given array reference of files (base names).

  $xfer->add_error( [ @files ] );

=item B<set_error>

Set state to C<error> of given array reference of files (base names).

  $xfer->set_error( [ @files ] );

=item B<get_final_files>

Return a array reference of files with C<final> state, given a partial file
name.

  $files = $xfer->get_final_files( 20100612 );

=item B<add_final>

Add state of C<final> of given array reference of files (base names).

  $xfer->add_final( [ @files ] );

=item B<set_final>

Set state to C<final> of given array reference of files (base names).

  $xfer->set_final( [ @files ] );

=item B<get_found_files>

Return a array reference of files with C<found> state, given a partial file
name.

  $files = $xfer->get_found_files( 20100612 );

=item B<add_found>

Add state of C<found> of given array reference of files (base names).

  $xfer->add_found( [ @files ] );

=item B<set_found>

Set state to C<found> of given array reference of files (base names).

  $xfer->set_found( [ @files ] );

=item B<get_ignored_files>

Return a array reference of files with C<ignored> state, given a partial file
name.

  $files = $xfer->get_ignored_files( 20100612 );

=item B<add_ignored>

Add state of C<ignored> of given array reference of files (base names).

  $xfer->add_ignored( [ @files ] );

=item B<set_ignored>

Set state to C<ignored> of given array reference of files (base names).

  $xfer->set_ignored( [ @files ] );

=item B<get_ingested_files>

Return a array reference of files with C<ingested> state, given a partial file
name.

  $files = $xfer->get_ingested_files( 20100612 );

=item B<add_ingested>

Add state of C<ingested> of given array reference of files (base names).

  $xfer->add_ingested( [ @files ] );

=item B<set_ingested>

Set state to C<ingested> of given array reference of files (base names).

  $xfer->set_ingested( [ @files ] );

=item B<get_replicated_files>

Return a array reference of files with C<replicated> state, given a partial file
name.

  $files = $xfer->get_replicated_files( 20100612 );

=item B<add_replicated>

Add state of C<replicated> of given array reference of files (base names).

  $xfer->add_replicated( [ @files ] );

=item B<set_replicated>

Set state to C<replicated> of given array reference of files (base names).

  $xfer->set_replicated( [ @files ] );

=item B<get_simulation_files>

Return a array reference of files with C<simulation> state, given a partial file
name.

  $files = $xfer->get_simulation_files( 20100612 );

=item B<add_simulation>

Add state of C<simulation> of given array reference of files (base names).

  $xfer->add_simulation( [ @files ] );

=item B<set_simulation>

Set state to C<simulation> of given array reference of files (base names).

  $xfer->set_simulation( [ @files ] );

=item B<get_transferred_files>

Return a array reference of files with C<transferred> state, given a partial file
name.

  $files = $xfer->get_transferred_files( 20100612 );

=item B<add_transferred>

Add state of C<transferred> of given array reference of files (base names).

  $xfer->add_transferred( [ @files ] );

=item B<set_transferred>

Set state to C<transferred> of given array reference of files (base names).

  $xfer->set_transferred( [ @files ] );

=cut

    for my $key ( sort keys %_state ) {

      my $set = qq[set_${key}];
      my $add = qq[add_${key}];
      my $get = qq[get_${key}_files];

      no strict 'refs';
      *$set =
        sub {
           my ( $self, $files ) = @_;

           return $self->_change_add_state( 'mode' => 'change',
                                            'state' => $key, 'file' => $files );
        };

      *$add =
        sub {
           my ( $self, $files ) = @_;

           return $self->_change_add_state( 'mode' => 'add',
                                            'state' => $key, 'file' => $files );
        };

      *$get =
        sub {
          my ( $self, %filter ) = @_;

          return $self->_get_files( %filter, 'state' => $key );
        };
    }
}
our %_state;

my %_rev_state;
while ( my ( $k, $v ) = each %_state ) {

  push @{ $_rev_state{  $v } }, $k;
}

sub code_to_descr {

  my ( $code ) = @_;

  return unless exists $_rev_state{ $code };
  return $_rev_state{ $code };
}

sub descr_to_code {

  my ( $descr ) = @_;

  return unless exists $_state{ $descr };
  return $_state{ $descr };
}

=item I<get_files_not_end_state>

Returns a list of files not in transferred state at CADC.

  $all = $xfer->get_files_not_end_state();

It takes an optional file name SQL pattern to return only the matching
files.

  $jun12 = $xfer->get_files_not_end_state( '%20100612%' );

=cut

sub get_files_not_end_state {

  my ( $self, $pattern ) = @_;

  my @state =
    @_state{qw[ deleted
                transferred
              ]
            };

  my $sql =
    sprintf
      qq[SELECT s.file_id, s.status, d.descr
          FROM $_state_table s , $_state_descr_table d
          WHERE s.status NOT IN ( %s )
          AND s.status = d.state
        ],
        join ', ', ( '?' ) x scalar @state
        ;

  $sql .= ' AND file_id like ? '
    if $pattern;

  $sql .= ' ORDER BY file_id';

  my $dbh = $self->_dbhandle();
  my $out = $dbh->selectall_hashref( $sql, 'file_id', undef,
                                      @state,
                                      ( $pattern ? $pattern : () )
                                    )
      or throw JSA::Error::DBError $dbh->errstr;

  return
    unless $out && keys %{ $out };

  return $out;
}

=item B<mark_transferred_as_deleted>

I<Marks> given list of files as I<deleted> which have been already marked as
being transferred.

  $xfer->mark_transferred_as_deleted( [ @file ] );

=cut

sub mark_transferred_as_deleted {

  my ( $self, $files ) = @_;

  my $sql =
    sprintf qq[UPDATE %s SET status = '%s' WHERE status = ? AND file_id IN (%s)],
    $_state_table,
    $_state{'deleted'},
    join ', ', ( '?' ) x scalar @{ $files };

  # Use the same $dbh during a transaction.
  my $dbh = $self->_dbhandle();

  my $log = Log::Log4perl->get_logger();
  $log->info( 'Before marking files as deleted' );

  $dbh->begin_work if $self->_use_trans();

  my @file = sort @{ $files };
  my @alt  = map { _fix_file_name( $_ ) } @file;

  $log->debug( join( "  \n", @alt ) );

  my @affected;
  my $affected = $self->_run_change_sql( $sql, $_state{'transferred'}, @file );
  push @affected, $affected if $affected;

  $dbh->commit if $self->_use_trans();

  my $sum = 0;
  $sum += $_ for @affected;
  return $sum;
}

=item B<name>

Returns the name of the table in which to collect, change states for
raw files.

  $name = JSA::DB::TableTransfer->name();

=cut

sub name { return $_state_table }

=item B<_dbhandle>

Returns the database handle.

  $dbh = $xfer->_dbhandle();

=cut

sub _dbhandle {

  my $self = shift @_;

  return $self->{'dbhandle'};
}

sub _use_trans {

  my $self = shift @_;

  return $self->{'transactions'};
}

sub _get_files {

  my ( $self, %filter ) = @_;

  my ( $state, $date, $instr ) = _extract_filter( %filter );

  my $fragment = sprintf '%s%%', join '%', grep { $_ } $instr, $date;

  my $log = Log::Log4perl->get_logger();
  $log->info( "Getting files from JAC database with state '${state}'" );

  my $dbh = $self->_dbhandle();

  return
    $self->_run_select_sql( $dbh,
                            'state' => $_state{ $state },
                            'file' => $fragment
                          );
}

sub _extract_filter {

  my ( %filter ) = @_;

  my ( $state, $date, $instr ) =
    @filter{qw[ state date instrument ]};

  _check_state( $state );
  _check_filename_part( $date ) if $date;

  $instr = _translate_instrument( $instr );

  return ( $state, $date, $instr );
}

sub _translate_instrument {

  my ( $instr ) = @_;

  return '' unless $instr;
  return 's' if $instr =~ m/^scuba-?2\b/i;
  return 'a' if $instr =~ m/^acsis\b/i;

  throw JSA::Error::BadArgs qq[Unknown instrument, '$instr', given.];
}

sub _check_filename_part {

  my ( $part ) = @_;

  my $parse_date =
    qr/ ^
        2\d{3}
        (?: 0[1-9] | 1[0-2] )
        (?: 0[1-9] | [12][0-9] | 3[01] )
        $
      /x;

  return
    if $part
    && length $part > 1
    && (  # Assume partial file name to be a date if it is a 8-digit number.
          $part =~ m/^\d{8}/
          ? $part =~ $parse_date
          : 1
        );

  throw JSA::Error::BadArgs 'No valid date given to check for files.';
}

{
  my $suffix_re;

  sub _fix_file_name {

    my ( $file ) = @_;

    my $suffix   = '.sdf';
    $suffix_re ||= qr/$_$/ for quotemeta $suffix;

    return
      $file =~ $suffix_re
      ? $file
      : qq[${file}${suffix}]
      ;
  }
}

sub _run_select_sql {

  my ( $self, $dbh, %bind ) = @_;

  my ( $file, $state ) = @bind{qw[ file state ]};

  my $sql = qq[SELECT file_id from $_state_table];
  if ( any { $_ } ( $file, $state ) ) {

    $sql .=
      ' WHERE'
      . join ' AND ',
          $file ? ' file_id like ?' : (),
          $state ? ' status = ?' : (),
  }
  $sql .= ' ORDER BY file_id';

  my $out = $dbh->selectall_arrayref( $sql, undef, $file, $state )
      or throw JSA::Error::DBError $dbh->errstr;

  return
    unless $out && scalar @{ $out };

  return _simplify_arrayref( $out );
}

sub _simplify_arrayref {

  my ( $in ) = @_;

  return
    unless $in && scalar @{ $in };

  return
    [ map { $_->[0] } @{ $in } ];
}

sub _change_add_state {

  my ( $self, %args ) = @_;

  my ( $mode, $files, $state ) =
    map { $args{ $_ } } qw[ mode file state ];

  eval { _check_state( $state ) }; croak $@ if $@;

  # Use the same $dbh during a transaction.
  my $dbh = $self->_dbhandle();

  $dbh->begin_work if $self->_use_trans();

  my $run =
    'add' eq $mode
    ? sub { return $self->_insert( @_ ) ; }
    : sub { return $self->_update( @_ ) ; }
    ;

  my $log = Log::Log4perl->get_logger();
  $log->info(
              ( 'add' eq $mode
                ? qq[Before adding]
                : qq[Before setting]
              ),
              qq[ '${state}' state for files\n]
            );

  my @affected;
  for my $file ( sort @{ $files } ) {

    my $alt = _fix_file_name( $file );

    $log->debug( qq[  ${alt}\n] );

    # Explicitly pass $dbh.
    my $affected = $run->( 'file'     => $alt,
                            'state'   => $state
                          );

    push @affected, $affected if $affected;
  }

  $dbh->commit if $self->_use_trans();

  my $sum = 0;
  $sum += $_ for @affected;
  return $sum;
}

sub _insert {

  my ( $self, %arg ) = @_;

  my $sql =
    qq[INSERT INTO $_state_table ( file_id, status ) VALUES ( ? , ? )];

  return
    $self->_run_change_sql( $sql,
                            map { $arg{ $_ } } qw[ file state ]
                          );
}

sub _update {

  my ( $self, %arg ) = @_;

  my $sql =
    qq[UPDATE $_state_table SET status = ? WHERE file_id = ?];

  my ( @bind ) = map { $arg{ $_ } } qw[ state file ];

  # Avoid resetting 't' multiple times when the transferred file list is same as
  # the list generated by jcmtInfo, where state of some of the files would have
  # been already been changed to transferred.
  #
  # As of Jul 16, 2010, other of the states is changed from one to the other, so
  # same rows are not updated with the old information.
  if ( 't' eq $arg{'state'} ) {

    $sql = join ' AND ', $sql, q[state <> ?];
    push @bind, $arg{'state'};
  }

  return $self->_run_change_sql( $sql, @bind );
}

sub _run_change_sql {

  my ( $self, $sql, @bind ) = @_;

  my $dbh = $self->_dbhandle();

  return $dbh->do( $sql, undef, @bind )
            or do {
                    $dbh->rollback;
                    throw JSA::Error::DBError $dbh->errstr;
                  };
}

sub _check_hashref {

  my ( $h ) = @_;

  return $h && ref $h && keys %{ $h };
}

sub _check_state {

  my ( $type ) = @_;

  return if exists $_state{ $type };

  throw JSA::Error::BadArgs
    sprintf "Unknown state type, %s, given, exiting ...\n",
      ( defined $type ? $type : 'undef' );
}


1;

__END__

=back

=head1 AUTHOR

Anubhav <a.agarwal@jach.hawaii.edu>

=head1 COPYRIGHT

Copyright (C) 2010 Science and Technology Facilities Council.
All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 2 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful,but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place,Suite 330, Boston, MA  02111-1307, USA

=cut

