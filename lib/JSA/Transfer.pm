package JSA::Transfer;

=pod

=head1 NAME

JSA::Transfer - Check file replication, transfer files to CADC

=head1 SYNOPSIS

Find replicated files in CADC database ...

...

Copy replicated files to CADC ...

...

"Finish" transfer by consulting CADC if it has got all the files,
update file status accordingly ...

...

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

sub new {

  my ( $class, %arg ) = @_;

  my $obj = { };
  for my $k ( keys %_config ) {

    $obj->{ $k } =
      exists $arg{ $k } ? $arg{ $k } : $_config{ $k } ;
  }

  return bless $obj, $class;
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

    for my $key ( qw[ found error ignored ingested replicated copied transferred ] ) {

      my $set = qq[set_${key}];
      my $get = qq[get_${key}_files];

      no strict 'refs';
      *$set =
        sub {
           my ( $self, $files ) = @_;

           return $self->_set_status( $key, $files );
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


sub verbose {

  my $self = shift @_;

  return $self->{'verbose'} unless scalar @_;

  $self->{'verbose'} = shift @_;
  return;
}

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
    $self->_run_select_sql( 'select', $dbh, $_status{ $type }, $fragment )
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

  my ( $self, $dbh, $type, @bind ) = @_;

  my $sql =
    qq[ SELECT file_id from $_status_table
        WHERE file_id like ? AND status = ?
        ORDER BY file_id
      ];

  my $out = $dbh->selectall_arrayref( $sql, undef, @bind )
      or croak $dbh->errstr;

  return
    unless $out && scalar @{ $out };

  return $out;
}

sub _set_status {

  my ( $self, $type, $files ) = @_;

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

  $dbh->commit;

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

    my $dbh =
      DBI->connect( "dbi:Sybase:server=$server" , $user, $pass
                    , { 'RaiseError' => 1
                      , 'PrintError' => 0
                      , 'AutoCommit' => 0
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

=head1  DESCRIPTION

This program checks file replication status at CADC in database and on
disk for a given date (and an instrument in one case).  Based on file
status, it copies a file to CADC transfer directory.

Without any arguments, it process the files for current UTC date.
Provide any other UTC date in C<yyyymmdd> format via I<-date> option.

=head1 OPTIONS

Specifying any *-wait option will initiate an endless loop, and only
the related action will be taken.

=over 2

=item B<-help>

Show synopsis & options.

=item B<-man>

Show the complete help document.

=item B<-copy>

Put those files in CADC transfer directory which CADC database knows
about.

=item B<-replication>

Check if files with "ingested" status listed in JAC database have been
replicated in CADC database.  If they are, then copy the files to CADC
transfer directory; change the file status to "replicated".

=item B<-transfer>

Check if files with "replicated" status have appeared on disk at CADC.
If they have, change the status to "transferred".

=item B<-verbose>

Show verbose output; specify multiple times to increase verbosity.


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

