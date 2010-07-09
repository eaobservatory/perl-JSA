package JSA::Transfer;

=pod

=head1 NAME

JSA::Transfer - Check file replication, transfer files to CADC

=head1 SYNOPSIS

Make an object ...

  $xfer = JSA::Transfer->new( 'verbose' => 1 );

Pass jcmt database handle already being used if desired ...

  $xfer->dbhandle( $dbh );

Set replicated status for a file ...

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
& table replication tracking processes update the status. Disk cleaning process
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

use JAC::Setup qw[ omp ];

#BEGIN {
#
#  our $omp_lib =
#    #'/jac_sw/omp/msbserver'
#    '/home/agarwal/src/jac-git/omp-perl'
#    ;
#
#  $ENV{'OMP_CFG_DIR'} = File::Spec->catfile( $omp_lib, 'cfg' );
#
#}
#our $omp_lib;
#
#use lib
#  $omp_lib,
#  '/home/agarwal/src/jac-git/perl-JSA/lib',
#  ;

use OMP::Config;

$OMP::Config::DEBUG = 0;

my %_config =
  (
    'verbose' => 0,

    'db-config' =>
      '/home/jcmtarch/enterdata-cfg/enterdata.cfg',
      #'/home/agarwal/src/jac-git/archiving/jcmt/.enterdata-cfg/enterdata.cfg',
  );

my $_status_table = 'transfer';

=head1 METHODS

All the C<get_*_files> methods will be changed to not require partial file name
in future.  All of C<get_*_files> and C<set_*> methods may be replaced with
versions which require a status type, similar to I<add_status> method.

The status types are ...

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
  our %_status =
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

Return a array reference of files with C<copied> status, given a partial file
name.

  $files = $xfer->get_copied_files( 20100612 );

=item B<set_copied>

Set status to C<copied> of given array reference of files (base names).

  $xfer->set_copied( [ @files ] );

=item B<get_error_files>

Return a array reference of files with C<error> status, given a partial file
name.

  $files = $xfer->get_error_files( 20100612 );

=item B<set_error>

Set status to C<error> of given array reference of files (base names).

  $xfer->set_error( [ @files ] );

=item B<get_found_files>

Return a array reference of files with C<found> status, given a partial file
name.

  $files = $xfer->get_found_files( 20100612 );

=item B<set_found>

Set status to C<found> of given array reference of files (base names).

  $xfer->set_found( [ @files ] );

=item B<get_ignored_files>

Return a array reference of files with C<ignored> status, given a partial file
name.

  $files = $xfer->get_ignored_files( 20100612 );

=item B<set_ignored>

Set status to C<ignored> of given array reference of files (base names).

  $xfer->set_ignored( [ @files ] );

=item B<get_ingested_files>

Return a array reference of files with C<ingested> status, given a partial file
name.

  $files = $xfer->get_ingested_files( 20100612 );

=item B<set_ingested>

Set status to C<ingested> of given array reference of files (base names).

  $xfer->set_ingested( [ @files ] );

=item B<get_replicated_files>

Return a array reference of files with C<replicated> status, given a partial file
name.

  $files = $xfer->get_replicated_files( 20100612 );

=item B<set_replicated>

Set status to C<replicated> of given array reference of files (base names).

  $xfer->set_replicated( [ @files ] );

=item B<get_transferred_files>

Return a array reference of files with C<transferred> status, given a partial file
name.

  $files = $xfer->get_transferred_files( 20100612 );

=item B<set_transferred>

Set status to C<transferred> of given array reference of files (base names).

  $xfer->set_transferred( [ @files ] );

=cut

    for my $key ( qw[ found error ignored ingested replicated copied transferred ] ) {

      my $set = qq[set_${key}];
      my $get = qq[get_${key}_files];

      no strict 'refs';
      *$set =
        sub {
           my ( $self, $files ) = @_;

           return $self->_set_status( 'status' => $key, 'file' => $files );
        };

        *$get =
          sub {
            my ( $self, $frag ) = @_;

            return $self->_get_files( $frag, $key );
          };
    }

}
our %_status;

my %_rev_status;
while ( my ( $k, $v ) = each %_status ) {

  push @{ $_rev_status{  $v } }, $k;
}

=item B<add_status>

Add rows in the table with file names and status, given a status type and an
array reference of base file names.

  $xfer->add_status( 'ingested', [ $file ] );

=cut

sub add_status {

  my ( $self, $type, $files ) = @_;

  exists $_status{ $type }
    or croak sprintf "Unknown status type, %s, given, exiting ...",
              ( defined $type ? $type : 'undef' );

  croak "A populated array reference was expected."
    unless $files
    && ref $files
    && scalar @{ $files };

  return
    $self->_change_add_status( 'insert',
                                { map { $_ => $_status{ $type } } @{ $files } }
                              );
}

sub code_to_descr {

  my ( $code ) = @_;

  return unless exists $_rev_status{ $code };
  return $_rev_status{ $code };
}

sub descr_to_code {

  my ( $descr ) = @_;

  return unless exists $_status{ $descr };
  return $_status{ $descr };
}


=item B<verbose>

If nothing is given, then returns the current verbosity level.

  $xfer->verbose() and warn "Verbose mode set";

Sets verbosity level, given an integer.

  $xfer->verbose( 2 );

=cut

sub verbose {

  my $self = shift @_;

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

  $self->verbose() and warn "Setting external database handle\n";

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

  my ( $self, $fragment, $type ) = @_;

  exists $_status{ $type }
    or croak sprintf 'Unknown status type, %s, given.',
              ( defined $type ? $type : 'undef' );

  $self->_check_filename_part( $fragment )
    or croak 'No valid date or other file name fragment given to check for files.';

  $fragment = join $fragment, ( '%' ) x 2;

  $self->verbose()
    and warn "Getting files from JAC database with status of ${type}\n";

  my $dbh = $self->dbhandle();

  my $files =
    $self->_run_select_sql( $dbh,
                            'status' => $_status{ $type },
                            'file' => $fragment
                          )
      or return;

  return
    { map { $_->[0] => undef } @{ $files } };
}

sub _check_filename_part {

  my ( $self, $part ) = @_;

  my $parse_date =
    qr/ ^
        2\d{3}
        (?: 0[1-9] | 1[0-2] )
        (?: 0[1-9] | [12][0-9] | 3[01] )
        $
      /x;

  return
    $part
    && length $part > 1
    && (  # Assume partial file name to be a date if it is a 8-digit number.
          $part =~ m/^\d{8}/
          ? $part =~ $parse_date
          : 1
        );
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

  my ( $file, $status ) =
    @bind{qw[ file status ]};

  my $sql =
    qq[ SELECT file_id from $_status_table
        WHERE file_id like ? AND status = ?
        ORDER BY file_id
      ];

  my $out = $dbh->selectall_arrayref( $sql, undef, $file, $status )
      or croak $dbh->errstr;

  return
    unless $out && scalar @{ $out };

  return $out;
}

sub _set_status {

  my ( $self, %bind ) = @_;

  my ( $files, $type ) =
    @bind{qw[ file status ]};

  exists $_status{ $type }
    or croak sprintf "Unknown status type, %s, given, exiting ...",
              ( defined $type ? $type : 'undef' );

  $self->verbose() and warn qq[Setting "${type}" status\n];

  return
    $self->_change_add_status( 'update',
                                { map { $_ => $_status{ $type } } @{ $files } }
                              );
}

sub _change_add_status {

  my ( $self, $mode, $file_status ) = @_;

  croak "A populated hash reference was expected."
    unless _check_hashref( $file_status );

  $self->verbose() and warn qq[Setting status for files\n];

  # Use the same $dbh during a transaction.
  my $dbh = $self->dbhandle();

  my $run = "_${mode}";

  $dbh->begin_work if $self->use_transaction();

  my @affected;
  for my $file ( keys %{ $file_status } ) {

    my $alt = _fix_file_name( $file );

    my $status = $file_status->{ $file };

    $self->verbose() > 1
      and warn qq[  Setting status of '${status}' for ${alt}\n];

    # Explicitly pass $dbh.
    my $affected = $self->$run( 'dbhandle' => $dbh,
                                'file'     => $alt,
                                'status'   => $status
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
    qq[INSERT INTO $_status_table ( file_id, status ) VALUES ( ? , ? )];

  return
    $self->_run_change_sql( $sql,
                            map { $arg{ $_ } } qw[ dbhandle file status ]
                          );
}

sub _update {

  my ( $self, %arg ) = @_;

  my $sql =
    qq[UPDATE $_status_table SET status = ? WHERE file_id = ?];

  return
    $self->_run_change_sql( $sql,
                            map { $arg{ $_ } } qw[ dbhandle status file ]
                          );
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

    my $key = join ':', ( $server, $db, $user, $pass );
    $key = crypt( $key, $pass );

    $self->verbose() and warn "Connecting to ${server}.${db} as ${user}\n";

    if ( exists $_handles{ $key } ) {

      $self->verbose() and warn "(found cached connection)\n";

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

      $v and $v->disconnect;
    }
    return
  }

}

END { _release_dbh(); }


sub _check_hashref {

  my ( $h ) = @_;

  return $h && ref $h && keys %{ $h };
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

