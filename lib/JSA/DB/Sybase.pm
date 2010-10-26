package JSA::DB::Sybase;

=pod

=head1 NAME

JSA::DB::Sybase - Connects to a Sybase database server

=head1 SYNOPSIS

  use JSA::DB::Sybase qw[ connect_to_db ];

  $dbh = connect_to_db( $ini_config_file );


=head1 DESCRIPTION

This package provides a database handle given "ini" style database log in
configuration with "database" section.  See L<OMP::Config> for details.

Database handles are released at the end of the program via C<END>.

=head1 FUNCTIONS

=over 2

=cut

use strict; use warnings;

use Carp qw[ croak ];
use Exporter qw[ import ];
use Log::Log4perl;

use JAC::Setup qw[ omp ];

use OMP::Config;

our @EXPORT_OK = qw[ connect_to_db ];

$OMP::Config::DEBUG = 0;

{
  my $omp_cf;
  my %_handles;

=item B<connect_to_db>

Returns a database handle given a C<ini> style database configuration
file with C<database> as the section (see L<OMP::Config> for details).

  $dbh = connect_to_db( $ini_config_file );

Optionally takes a "name" to differentiate one instance from another
given same database configuration.

  $jsa_dbh = connect_to_db( $ini_config_file, 'JSA::DB' );

Returns a cached handle if a handle has already been created for a
set of configuration.

Sets C<$dbh-E<gt>{'RaiseError'}> and C<$dbh-E<gt>{'AutoCommit'}> (see L<DBI>).

Throws error if cannot connect to the server or if cannot switch to
given database.

=cut

  sub connect_to_db {

    my ( $config, $name ) = @_;

    $omp_cf ||= OMP::Config->new;

    $omp_cf->configDatabase( $config );

    my ( $server, $db, $user, $pass ) =
      map
        { $omp_cf->getData( "database.$_" ) }
        qw[ server  database  user  password ];

    my $key = join ':', ( $name ? $name : '', $server, $db, $user );

    my $log = Log::Log4perl->get_logger( '' );
    $log->info( "Connecting to ${server}..${db} as ${user}\n" );

    if ( exists $_handles{ $key } && $_handles{ $key } ) {

      $log->trace( "  found cached connection" );
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
        or $log->logcroak( $DBI::errstr );

    for ( $dbh )
    {
      $_->{'syb_show_sql'} = 1 ;
      $_->{'syb_show_eed'} = 1 ;

      $_->do( "use $db" ) or $log->logdie( $_->errstr );
    }

    $_handles{ $key } = $dbh;

    return $dbh ;
  }

  sub _release_dbh {

    my $log = Log::Log4perl->get_logger( '' );

    for my $k ( keys %_handles ) {

      my $v = $_handles{ $k };

      if ( $v ) {

        $log->info( "Disconnecting from $k" );

        $v->disconnect();
        undef $v;
      }
    }
    return
  }

}

END { _release_dbh(); }


1;

__END__

=pod

=back

=head1 SEE ALSO

=over 2

=item *

L<DBI>

=item *

L<OMP::Config>

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

