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

use JSA::Error qw[ :try ];
use OMP::Config;

$OMP::Config::DEBUG = 0;

my $_state_table = 'transfer';
my $_state_descr_table = 'transfer_state';

=head1 METHODS

All the C<get_*_files> methods will be changed to not require partial file name
in future.

The state types are ...

  copied_pre_cadc (copied to intermediate directory before moving to CADC)
  copied
  delete  (mark as deletion candidate)
  deleted
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

      #  File has been put in intermediate directory.
      'copied_pre_cadc' => 'p',

      #  File present in CADC directory.
      'transferred' => 't',

      'final' => 'z',
    );

=item B<get_copied_pre_cadc_files>

Return a array reference of files with C<copied_pre_cadc> state, given a partial
file name.

  $files = $xfer->get_copied_pre_cadc_files( 20100612 );

=item B<add_copied_pre_cadc>

Add state of C<copied_pre_cadc> of given array reference of files (base names).

  $xfer->add_copied_pre_cadc( [ @files ] );

=item B<set_copied_pre_cadc>

Set state to C<copied_pre_cadc> of given array reference of files (base names).

  $xfer->set_copied_pre_cadc( [ @files ] );

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
      my $put = qq[put_${key}];
      my $get = qq[get_${key}_files];

      next
        if $key eq 'found';

      no strict 'refs';
      *$get =
        sub {
          my ( $self, %filter ) = @_;

          return $self->_get_files( %filter, 'state' => $key );
        };

      *$set =
        sub {
          my ( $self, $files, $text ) = @_;

          my %opt;
          2 < scalar @_ and $opt{'comment'} = $text;

          return $self->_change_add_state(  'mode'    => 'change',
                                            'state'   => $key,
                                            'file'    => $files,
                                            %opt,
                                          );
        };

      *$add =
        sub {
          my ( $self, $files, $text ) = @_;

          my %opt;
          2 < scalar @_ and $opt{'comment'} = $text;

          return $self->_change_add_state( 'mode'    => 'add',
                                            'state'   => $key,
                                            'file'    => $files,
                                            %opt,
                                          );
        };

      *$put =
        sub {
          my ( $self, $files, $text ) = @_;

          my %opt;
          2 < scalar @_ and $opt{'comment'} = $text;

          return $self->_put_state(  'state'   => $key,
                                      'file'    => $files,
                                      %opt,
                                    );
        };
    }
}
our %_state;

my %_rev_state;
while ( my ( $k, $v ) = each %_state ) {

  push @{ $_rev_state{  $v } }, $k;
}

=item B<get_found_files>

Return a array reference of files with C<found> state, given a partial file
name.

  $files = $xfer->get_found_files( 20100612 );

=cut

sub get_found_files {

  my ( $self, $pattern ) = @_;

  my $sql =
    sprintf
      qq[ SELECT s.file_id , s.location
          FROM $_state_table s , $_state_descr_table d
          WHERE s.status = ?
            AND s.status = d.state
            AND s.location IS NOT NULL
        ]
        ;

  $sql .= ' AND file_id like ? '
    if $pattern;

  $sql .= ' ORDER BY s.file_id, s.location ';

  my $dbh = $self->_dbhandle();
  my $out = $dbh->selectcol_arrayref( $sql,
                                      # Return only file paths.
                                      { 'Columns' => [2] },
                                      $_state{'found'},
                                      ( $pattern ? $pattern : () )
                                    )
                                  or throw JSA::Error::DBError $dbh->errstr;


  return $out;
}

=item B<add_found>

Adds state of C<found> of given array reference of files (absolute
paths).

  $xfer->add_found( [ @files ] );

=cut

sub add_found {

  my ( $self, $files ) = @_;

  my $vals = _process_paths( $files )
    or return;

  my $db = $self->_make_jdb();
  return
    $db->insert(  'table'   => $_state_table,
                  'columns' => [ 'file_id', 'status', 'location' ],
                  'values'  => $vals,
                );
}

=item B<set_found>

Updates state of C<found> of given array reference of files (absolute
paths).

  $xfer->set_found( [ @files ] );

=cut

sub set_found {

  my ( $self, $files ) = @_;

  return
    $self->put_found( $files, my $update_only = 1 );
}

=item B<put_found>

Adds or changes state of C<found> of given array reference of files
(absolute paths).

  $xfer->put_found( [ @files ] );

=cut

sub put_found {

  my ( $self, $files, $mod_only ) = @_;

  my $vals = _process_paths( $files )
    or return;

  my $db = $self->_make_jdb();
  return
    $db->update_or_insert(  'table'       => $_state_table,
                            'unique-keys' => [ 'file_id' ],
                            'columns'     => [ 'file_id', 'status', 'location' ],
                            'values'      => $vals,
                            'update-only' => $mod_only,
                          );
}

sub _process_paths {

  my ( $paths ) = @_;

  my @path = @{ $paths };

  return unless scalar @path;

  my $state = $_state{'found'};

  require File::Basename;
  import File::Basename qw[ fileparse ];

  my @val;
  for my $f ( @path ) {

    my $alt = _fix_file_name( ( fileparse( $f, '' ) )[0] );
    push @val, [ $alt, $state, $alt eq $f ? undef : $f ];
  }

  return unless scalar @val;
  return [ @val ];
}

=item B<code_to_descr>

Returns a descriptive text given a state code.

  $text = JSA::DB::TableTransfer( 'f' );

=cut

sub code_to_descr {

  my ( $code ) = @_;

  return unless exists $_rev_state{ $code };
  return $_rev_state{ $code };
}

=item B<code_to_descr>

Returns a state code given kwown descriptive text.

  $code = JSA::DB::TableTransfer( 'found' );

=cut

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
                simulation
                ignored
              ]
            };

  my $sql =
    sprintf
      qq[SELECT s.file_id, s.status, d.descr, s.error, s.comment,
         -- Extract date from file name.
         CASE
            WHEN SUBSTRING( file_id, 1, 1 ) = 's'
              -- SCUBA-2 files.
              THEN SUBSTRING( file_id, 4, 8 )
           ELSE
              -- ACSIS files.
              SUBSTRING( file_id, 2, 8 )
          END AS date
          FROM $_state_table s , $_state_descr_table d
          WHERE (  s.status NOT IN ( %s )
                OR s.error = 1
                )
          AND s.status = d.state
        ],
        join ', ', ( '?' ) x scalar @state
        ;

  $sql .= ' AND file_id like ? '
    if $pattern;

  $sql .= ' ORDER BY date, file_id';

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

  my $log = Log::Log4perl->get_logger( '' );
  $log->info( 'Before marking files as deleted' );

  $dbh->begin_work if $self->_use_trans();

  my @file = sort @{ $files };
  my @alt  = map { _fix_file_name( $_ ) } @file;

  $log->debug( join( "  \n", @alt ) );

  my $affected = $self->_run_change_sql( $sql, $_state{'transferred'}, @file );

  $dbh->commit if $self->_use_trans();

  return $affected;
}

=item B<name>

Returns the name of the table in which to collect, change states for
raw files.

  $name = JSA::DB::TableTransfer->name();

=cut

sub name { return $_state_table }

=item B<unique_keys>

Returns list of columns to uniquely identify a row.

  @keys = JSA::DB::TableTransfer->unique_keys();

=cut

sub unique_keys {

  return
    qw[ file_id
      ];
}

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

  my $log = Log::Log4perl->get_logger( '' );

  my ( $state, $date, $instr ) = _extract_filter( %filter );
  my $jac = $filter{'keep_jac'};

  my @select = qw[ file_id comment ];
  push @select, 'location'
    if $state eq 'found';

  ( my $state_col, $state ) = _alt_state( $_state{ $state } );

  my %where;
  $where{qq[ $state_col = ?]} = $state;

  my $fragment = sprintf '%s%%', join '%', grep { $_ } $instr, $date;
  $where{' file_id like ?'} = $fragment if defined $fragment;

  $where{' keep_jac = ? '} = $jac if defined $jac;

  $log->info( "Getting files from JAC database with state '${state}'" );

  my $db = $self->_make_jdb();
  my $out =
    $db->select_loop( 'table'   => $_state_table,
                      'columns' => [ @select ],
                      'where'   => [ keys %where ],
                      'values'  => [[ values %where ]],
                    );

  return
    unless $out
    && ref $out && scalar @{ $out };

  return $db->_simplify_arrayref_hashrefs( $out );
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

  return lc qq[s${1}] if $instr =~ m/^s?([48][a-d])\b/i;

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

    ( my $state_col, undef ) = _alt_state( $state );

    my @where;
    push @where, ' file_id like ?'   if $file;
    push @where, qq[ $state_col = ?] if defined $state;

    $sql .= ' WHERE ' . join ' AND ', @where;
  }
  $sql .= ' ORDER BY file_id';

  my $out = $dbh->selectall_arrayref( $sql, undef, $file, $state )
      or throw JSA::Error::DBError $dbh->errstr;

  return
    unless $out && scalar @{ $out };

  require JSA::DB;
  return JSA::DB->_simplify_arrayref( $out );
}


# This would be eventual replacement of _change_add_state() after references to
# add*state() & set*state() have been updated.  This does not care if going to
# INSERT or UPDATE (JSA::DB->update_or_insert() takes care of that).
sub _put_state {

  my ( $self, %args ) = @_;

  my ( $files, $state ) =
    map { $args{ $_ } } qw[ file state ];

  eval { _check_state( $state ) }; croak $@ if $@;

  ( my $state_col, $state ) = _alt_state( $_state{ $state } );

  my $db = $self->_make_jdb();

  my @alt = map { _fix_file_name( $_ ) } sort @{ $files };
  return
    $db->update_or_insert(  'table'       => $self->name(),
                            'unique-keys' => [ 'file_id' ],
                            'columns'     => [ 'file_id', $state_col, 'comment' ],
                            'values'      => [ map { [ $_, $state, $args{'comment'} ] } @alt ],
                            'dbhandle'    => $self->_dbhandle(),
                          );
}

sub _change_add_state {

  my ( $self, %args ) = @_;

  my ( $mode, $files, $state ) =
    map { $args{ $_ } } qw[ mode file state ];

  eval { _check_state( $state ) }; croak $@ if $@;

  ( my $state_col, $state  ) = _alt_state( $_state{ $state } );

  # Use the same $dbh during a transaction.
  my $dbh = $self->_dbhandle();

  $dbh->begin_work if $self->_use_trans();

  my $run =
    'add' eq $mode
    ? sub { return $self->_insert( @_ ) ; }
    : sub { return $self->_update( @_ ) ; }
    ;

  my $log = Log::Log4perl->get_logger( '' );
  $log->info(
              ( 'add' eq $mode
                ? qq[Before adding]
                : qq[Before setting]
              ),
              qq[ '${state}' state for files\n]
            );
  $log->debug( '  ' . join "\n  ", map { $_ } @{ $files } );

  my @affected;
  for my $file ( sort @{ $files } ) {

    my $alt = _fix_file_name( $file );

    #$log->debug( qq[  ${alt}\n] );

    # Explicitly pass $dbh.
    my $affected = $run->( 'file'       => $alt,
                            'state'     => $state,
                            'state-col' => $state_col,
                            'comment'   => $args{'text'},
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

  my $state_col = $arg{'state-col'} || 'status';

  my $sql =
    qq[INSERT INTO $_state_table ( file_id, $state_col, comment ) VALUES ( ? , ?  , ? )];

  return
    $self->_run_change_sql( $sql,
                            map { $arg{ $_ } } qw[ file state comment ]
                          );
}

sub _update {

  my ( $self, %arg ) = @_;

  my $state_col = $arg{'state-col'} || 'status';

  my $sql =
    qq[UPDATE $_state_table SET $state_col = ?, comment = ? WHERE file_id = ?];

  my ( @bind ) = map { $arg{ $_ } } qw[ state comment file ];

  # Avoid resetting 't' multiple times when the transferred file list is same as
  # the list generated by jcmtInfo, where state of some of the files would have
  # been already been changed to transferred.
  #
  # As of Jul 16, 2010, other of the states is changed from one to the other, so
  # same rows are not updated with the old information.
  if ( 't' eq $arg{'state'} ) {

    $sql = join ' AND ', $sql, q[status <> ?];
    push @bind, $arg{'state'};
  }

  return $self->_run_change_sql( $sql, @bind );
}

sub _run_change_sql {

  my ( $self, $sql, @bind ) = @_;

  my $dbh = $self->_dbhandle();

  return $dbh->do( $sql, undef, @bind )
            or do {
                    $dbh->rollback if $self->_use_trans();
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

sub _alt_state {

  my ( $state ) = @_;

  my $re = qr/^ e (?:rr (?:or)? )? $/x;

  unless ( $state =~ $re ) {

    return ( 'status' => $state );
  }

  return ( 'error' => 1 );
}

sub _make_jdb {

  my ( $self ) = @_;

  require JSA::DB;

  my $dbh = $self->_dbhandle();
  return
    JSA::DB->new( 'name' => 'transfer-change-add',
                  ( $dbh && ref $dbh
                    ? ( 'dbhandle' => $dbh )
                    : ()
                  )
                );
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

