package JSA::Transfer;

=pod

=head1 NAME

JSA::Transfer - Check file replication, transfer files to CADC

=head1 SYNOPSIS

Make an object ...

  $xfer = JSA::Transfer->new( 'verbose' => 1 );

Pass jcmt database handle already being used if desired ...

  $xfer->dbhandle( $dbh );

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

use Carp qw[ croak ];
use File::Spec;
use DateTime;
use DateTime::Duration;
use Pod::Usage;
use Getopt::Long qw[ :config gnu_compat no_ignore_case no_debug ];
use List::Util qw[ min sum ];
use List::MoreUtils qw[ any ];

use JAC::Setup qw[ omp ];

use OMP::Config;

$OMP::Config::DEBUG = 0;

my %_config =
  (
    'verbose' => 0,

    'db-config' =>
      '/home/jcmtarch/enterdata-cfg/enterdata.cfg',
      #'/home/agarwal/src/jac-git/archiving/jcmt/.enterdata-cfg/enterdata.cfg',
  );

my $_state_table = 'transfer';
my $_state_descr_table = 'transfer_state';

=head1 METHODS

All the C<get_*_files> methods will be changed to not require partial file name
in future.

The state types are ...

  copied
  error
  found
  ignored
  ingested
  replicated
  transferred

=over 2

=item B<new> constructor

Make a C<JSA::Transfer> object.  It takes a hash of two optional parameters ...

  verbose   - set verbosity level; default is 0;
  db-config - pass filename with database log in information in "ini" format;
              default is /home/jcmtarch/enterdata-cfg/enterdata.cfg.

  $xfer =
    JSA::Transfer->new( 'verbose'   => 1,
                        'db-config' =>
                          '/home/jcmtarch/enterdata-cfg/enterdata.cfg'
                      );

=cut

sub new {

  my ( $class, %arg ) = @_;

  my $obj = { };
  for my $k ( keys %_config ) {

    $obj->{ $k } =
      exists $arg{ $k } ? $arg{ $k } : $_config{ $k } ;
  }

  $obj = bless $obj, $class;
  $obj->use_transaction( 1 );

  return $obj;
}

BEGIN {
  our %_state =
    (
      #  File found on JAC disk (table has full path).
      'found' => 'f',

      #  Error causing file.
      'error' => 'e',

      'ignore'  => 'x',
      'ignored' => 'x',

      #  File has been locally ingested.
      'ingest'   => 'i',
      'ingested' => 'i',

      #  File appeared in CADC database.
      'replicate'  => 'r',
      'replicated' => 'r',

      #  File has been put in CADC transfer directory.
      'copy'   => 'c',
      'copied' => 'c',

      #  File present in CADC directory.
      'transfer'    => 't',
      'transferred' => 't',
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

    for my $key ( qw[ found error ignored ingested replicated copied transferred ] ) {

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

=item -

=cut

sub get_files_not_end_state {

  my ( $self ) = @_;

  my $sql =
    qq[SELECT s.file_id, s.status, d.descr
        FROM $_state_table s , $_state_descr_table d
        WHERE s.status <> ? AND s.status = d.state
        ORDER BY file_id
      ];

  my $dbh = $self->dbhandle();
  my $out = $dbh->selectall_hashref( $sql, 'file_id', undef, $_state{'transferred'} )
      or croak $dbh->errstr;

  return
    unless $out && keys %{ $out };

  return $out;
}

=item B<verbose>

If nothing is given, then returns the current verbosity level.

  $xfer->verbose() and warn "Verbose mode set";

Sets verbosity level, given an integer.

  $xfer->verbose( 2 );

=cut

sub verbose {

  my $self = shift @_;

  $self->{'verbose'} = 0
    unless defined $self->{'verbose'};

  return $self->{'verbose'} unless scalar @_;

  $self->{'verbose'} = 0 + shift @_;
  return;
}

=item B<dbhandle>

Returns the database handle if no value is given. If no handle is given, one is
generated internally from I<db-config> parameter (see I<new> method).

  $dbh = $xfer->dbhandle();

Override internal database handle to jcmt database ...

  $xfer->dbhandle( $dbh );

=cut

sub dbhandle {

  my $self = shift @_;

  my $extern = 'extern-dbh';

  unless ( scalar @_ ) {

    return
      exists $self->{ $extern } && ref $self->{ $extern }
      ? $self->{ $extern }
      : $self->_connect_to_db( $self->{'db-config'} )
      ;
  }

  $self->_make_noise( 0, "Setting external database handle\n" );

  $self->{ $extern } = shift @_;

  return;
}

=item B<use_transaction>

Purpose is to control database transactions externally in combination with
external database handle (see I<dbhandle> method).  Default is to use
transactions.

Returns a truth value to indicate if database transaction should be used, if no
value is given.

  $dbh = $xfer->use_transaction();

Else, given truth value is set for later use.

  $xfer->use_transaction( 1 );

=cut

sub use_transaction {

  my $self = shift @_;

  return $self->{'trans'} unless scalar @_;

  $self->{'trans'} = !! shift @_;
  return;
}


sub _get_files {

  my ( $self, %filter ) = @_;

  my ( $state, $date, $instr ) = _extract_filter( %filter );

  my $fragment = sprintf '%s%%', join '%', $instr, $date;

  $self->_make_noise( 1, "Getting files from JAC database with state '${state}'\n" );

  my $dbh = $self->dbhandle();

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
  _check_filename_part( $date );

  $instr = _translate_instrument( $instr );

  return ( $state, $date, $instr );
}

sub _translate_instrument {

  my ( $instr ) = @_;

  return '' unless $instr;
  return 's' if $instr =~ m/^scuba-?2\b/i;
  return 'a' if $instr =~ m/^acsis\b/i;

  croak qq[Unknown instrument, '$instr', given.];
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

  croak 'No valid date given to check for files.';
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
      or croak $dbh->errstr;

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
  my $dbh = $self->dbhandle();

  $dbh->begin_work if $self->use_transaction();

  my $run =
    'add' eq $mode
    ? sub { return $self->_insert( @_ ) ; }
    : sub { return $self->_update( @_ ) ; }
    ;

  $self->_make_noise( 0,
                      ( 'add' eq $mode
                        ? qq[Before adding]
                        : qq[Before setting]
                      ),
                      qq[ '${state}' state for files\n]
                    );

  my @affected;
  for my $file ( sort @{ $files } ) {

    my $alt = _fix_file_name( $file );

    $self->_make_noise( 1, qq[  ${alt}\n] );

    # Explicitly pass $dbh.
    my $affected = $run->( 'dbhandle' => $dbh,
                            'file'     => $alt,
                            'state'   => $state
                          );

    push @affected, $affected if $affected;
  }

  $dbh->commit if $self->use_transaction();

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
                            map { $arg{ $_ } } qw[ dbhandle file state ]
                          );
}

sub _update {

  my ( $self, %arg ) = @_;

  my $sql =
    qq[UPDATE $_state_table SET status = ? WHERE file_id = ?];

  my ( $dbh, @bind ) = map { $arg{ $_ } } qw[ dbhandle state file ];

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

  return $self->_run_change_sql( $sql, $dbh, @bind );
}

sub _run_change_sql {

  my ( $self, $sql, $dbh, @bind ) = @_;

  return $dbh->do( $sql, undef, @bind )
            or do {
                    $dbh->rollback;
                    croak $dbh->errstr;
                  };
}

{
  my $omp_cf;
  my %_handles;

  sub _connect_to_db {

    my ( $self, $config ) = @_;

    $omp_cf ||= OMP::Config->new;

    $omp_cf->configDatabase( $config );

    my ( $server, $db, $user, $pass ) =
      map
        { $omp_cf->getData( "database.$_" ) }
        qw[ server  database  user  password ];

    my $key = join ':', ( $server, $db, $user );

    $self->_make_noise( 2, "Connecting to ${server}..${db} as ${user}\n" );

    if ( exists $_handles{ $key } && $_handles{ $key } ) {

      $self->_make_noise( 2, "  found cached connection\n" );

      return $_handles{ $key };
    }

    require DBI;

    my $dbh =
      DBI->connect( "dbi:Sybase:server=$server" , $user, $pass,
                    { 'RaiseError' => 1,
                      'PrintError' => 0,
                      'AutoCommit' => 1,
                    }
                  )
        or die $DBI::errstr;

    for ( $dbh )
    {
      $_->{'syb_show_sql'} = 1 ;
      $_->{'syb_show_eed'} = 1 ;

      $_->do( "use $db" ) or croak $_->errstr;
    }

    $_handles{ $key } = $dbh;

    $self->use_transaction( 1 );

    return $dbh ;
  }

  sub _release_dbh {

    for my $v ( values %_handles ) {

      if ( $v ) {

        $v->disconnect() and undef $v;
      }
    }
    return
  }

}

END { _release_dbh(); }


sub _check_hashref {

  my ( $h ) = @_;

  return $h && ref $h && keys %{ $h };
}

sub _check_state {

  my ( $type ) = @_;

  return if exists $_state{ $type };

  croak sprintf "Unknown state type, %s, given, exiting ...\n",
            ( defined $type ? $type : 'undef' );
}

sub _make_noise {

  my ( $self, $min, @msg ) = @_;

  return unless scalar @msg;

  ( $min || 0 ) < $self->verbose()
    and print STDERR @msg;

  return;
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

