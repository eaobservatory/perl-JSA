package JSA::DB;

=pod

=head1 NAME

JSA::DB - Arranges for database handle given a configuration

=head1 SYNOPSIS

Make an object ...

  $jdb = JSA::DB->new( 'verbose' => 1 );

Pass jcmt database handle already being used if desired ...

  $jdb->dbhandle( $dbh );

=head1 DESCRIPTION

This package provides a database handle given "ini" style database log in
configuration with "database" section.  See L<OMP::Config> for details.

=cut

use strict; use warnings;

use List::Util qw[ sum max ];
use List::MoreUtils qw[ any ];

use JAC::Setup qw[ omp ];

use JSA::Error qw[ :try ];
use JSA::Verbosity;
use JSA::DB::Sybase qw[ connect_to_db ];

use OMP::Config;
use OMP::Constants qw [ :status ];

$OMP::Config::DEBUG = 0;

my %_config =
  (
    'verbose' => 0,

    'db-config' =>
      '/home/jcmtarch/enterdata-cfg/enterdata.cfg',
      #'/home/agarwal/src/jac-git/archiving/jcmt/.enterdata-cfg/enterdata.cfg',

    'name' => __PACKAGE__,

    'dbhandle' => undef,
  );

=head1 METHODS

=over 2

=item B<new> constructor

Make a C<JSA::DB> object.  It takes a hash of parameters ...

  db-config - pass file name with database log in information in "ini" format;
              default is /home/jcmtarch/enterdata-cfg/enterdata.cfg;

  name      - (optional) pass a string to differentiate db connections
              with same log in configuration; default is 'JSA::DB'.

  $jdb =
    JSA::DB->new( 'name'      => 'JSA::DB'
                  'db-config' =>
                    '/home/jcmtarch/enterdata-cfg/enterdata.cfg',
                );

Transactions are set to be used (see C<use_transaction> method).

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

=item B<dbhandle>

Returns the database handle if no value is given. If no handle is given, one is
generated internally from I<db-config> parameter (see I<new> method).

  $dbh = $jdb->dbhandle();

Override internal database handle to jcmt database ...

  $jdb->dbhandle( $dbh );

Passes up any errors thrown.

=cut

sub dbhandle {

  my $self = shift @_;

  my $extern = 'extern-dbh';

  unless ( scalar @_ ) {

    return $self->{ $extern }
      if exists $self->{ $extern }
      && ref $self->{ $extern };

    return connect_to_db( $self->{'db-config'}, $self->_name );
  }

  my $noise = JSA::Verbosity->new();
  $noise->make_noise( 0, "Setting external database handle." );

  $self->{ $extern } = shift @_;

  return;
}

=item B<use_transaction>

Purpose is to control database transactions externally in combination with
external database handle (see I<dbhandle> method).  Default is to use
transactions.

Returns a truth value to indicate if database transaction should be used, if no
value is given.

  $dbh = $jdb->use_transaction();

Else, given truth value is set for later use.

  $jdb->use_transaction( 1 );

=cut

sub use_transaction {

  my $self = shift @_;

  return $self->{'trans'} unless scalar @_;

  $self->{'trans'} = !! shift @_;
  return;
}

=item B<select_t>

Executes SELECT query given a hash of table names, array reference of
column names, where clauses with placeholders, and array reference of
bind values.

Returns number of affected rows if any.

  $affected =
    $jdb->select_t( 'table'   => 'the_table',
                  'columns' => [ 'a', 'b' ],
                  'where'   => [ a = ? AND b = ? ]
                  'values'  => [ 1, 2 ]
                );

Optional hash pairs are "order" & "group" respectively for C<ORDER BY>
and C<GROUP BY> clauses ...

  $affected =
    $jdb->select_t( 'table'   => 'the_table',
                     ...
                     'order' => [ 'b' , 'a' ]
                  );

On database error, rollbacks the transaction, errors are passed up.

(Note that I<select> is a built in function; "_t" in name refers to
database table.)

=cut

sub select_t {

  my ( $self, %arg ) = @_;

  _check_input( %arg,
                '_check_' =>
                { 'table'   => 0,
                  'columns' => 1,
                  'where'   => 0,
                  'values'  => 1,
                }
              );

  my $sql =
    sprintf 'SELECT %s FROM %s WHERE %s',
      ( map { _to_string( $_ ) } @arg{qw[ columns table ]}
      ),
      _where_string( $arg{'where'} )
    ;

  for my $opt (qw[ order group ] ) {

    next unless exists $arg{ $opt };

    $sql .= sprintf ' %s BY %s', uc $opt, _to_string( $arg{ $opt } );
  }

  return
    $self->run_select_sql( 'sql' => $sql,
                            'values' => $arg{'values'}
                          );
}

=item B<exist>

A special case of I<select_t>, returns a number to indicate if any
rows exist in given a hash of table names, "WHERE" clauses with
placeholders, and array reference of bind values.

  $count =
    $jdb->exist( 'table'   => [ 'transfer t', 'FILES f' ]
                  'where'  =>
                    q[      t.file_id = f.file_id
                        AND t.file_id like ?
                        AND t.state   = ?
                      ],
                  'values' => [ 'a20100324%', 't' ]
                );

=cut

sub exist {

  my ( $self, %arg ) = @_;

  my $result =
    $self->select_t( %arg, 'columns' => 'count(1) AS count' )
    or return 0;

  return $result->[0]{'count'};
}

=item B<run_select_sql>

Returns array reference of hash references (column name is key, column
value is value) given a hash of SQL and bind values.

Returns nothing if nothing is found.

  $result =
    $jdb->run_select_sql( 'sql' => q[SELECT ... WHERE a = ? AND b = ?],
                          'values' => [1, 2]
                        );

On database error, throws C<JSA::Error::DBError>.

=cut

sub run_select_sql {

  my ( $self, %arg ) = @_;

  my $dbh = $self->dbhandle;

  my @bind = @{ $arg{'values'} };

  my $out = $dbh->selectall_arrayref( $arg{'sql'}, { 'Slice' => {} }, @bind )
      or throw JSA::Error::DBError( $dbh->errstr );

  return unless $out && scalar @{ $out };
  return $out;
}

=item B<insert>

Executes INSERT query given a hash of table, array reference of column
names, and an array reference of value array references to be
inserted.

Returns number of affected rows if any.

  $affected =
    $jdb->insert( 'table'   => 'the_table',
                  'columns' => [ 'a', 'b' ],
                  'values'  => [ [1, 2], [4, 6] ]
                );

On database error, rollbacks the transaction, errors are passed up.

=cut

sub insert {

  my ( $self, %arg ) = @_;

  _check_input( %arg,
                '_check_' =>
                { 'table'   => 0,
                  'columns' => 1,
                  'values'  => 1,
                }
              );

  my @cols = @{ $arg{'columns'} };
  my $size = scalar @cols;

  my $sql =
    sprintf 'INSERT INTO % (%s) VALUES (%s)',
      $arg{'table'},
      join( ', ', @cols ),
      join( ', ', ('?') x $size )
      ;

  # Append the string if given, say something like
  # 'WHERE NOT EXIST IN ( SELECT ... )'.
  $sql .= ' ' . $arg{'_append'}
    if $arg{'_append'};

  return $self->_run_change_loop( 'sql'    => $sql,
                                  'values' => $arg{'values'}
                                );
}

=item B<update>

Executes UPDATE query given a hash table name, array reference of SET
clauses, WHERE clause string & and array reference of array references
of values for WHERE clause.

Returns number of affected rows if any.

  # Changes columns 'a' & 'b' of rows
  # where a is 1 & b is 2, or a is 4 & b is 6.
  $affected =
    $jdb->update( 'table'  => 'the_table',
                  'set'    => [ 'a = 4', 'b = b +1' ],
                  'where'  => 'a = ? AND b = ?',
                  'values' => [ [1, 2], [4, 6] ],
                );

On database error, transaction is undone, errors are passed up.

=cut

sub update {

  my ( $self, %arg ) = @_;

  _check_input( %arg,
                '_check_' =>
                { 'table'  => 0,
                  'set'    => 1,
                  'where'  => 0,
                  'values' => 1,
                }
              );

  my $sql =
    sprintf 'UPDATE %s SET %s WHERE %s',
      $arg{'table'},
      _to_string( $arg{'set'} ),
      _where_string( $arg{'where'} )
      ;

  return $self->_run_change_loop( 'sql'    => $sql,
                                  'values' => $arg{'values'}
                                );
}

=item B<delete>

Executes DELETE query given a hash table name, WHERE clause string &
and array reference of array references of values for WHERE clause.

Returns number of affected rows if any.

  # Deletes rows where a is 1, b is 2 OR a is 4 & b is 6.
  $affected =
    $jdb->delete( 'table'  => 'the_table',
                  'where'  => 'a = ? AND b = ?' ,
                  'values' => [ [1, 2], [4, 6] ],
                );

On database error, transaction is undone, errors are passed up.

=cut

sub delete {

  my ( $self, %arg ) = @_;

  _check_input( %arg,
                '_check_' =>
                { 'table'  => 0,
                  'where'  => 0,
                  'values' => 1,
                }
              );

  my $sql =
    sprintf 'DELETE %s WHERE %s',
      $arg{'table'},
      _where_string( $arg{'where'} )
      ;

  return $self->_run_change_loop( 'sql'    => $sql,
                                  'values' => $arg{'values'}
                                );
}

=item B<_run_change_loop>

Executes table change operations, given a hash of SQL query string and
array reference of array references of bind values (multiple second
level references will affect multiple rows).

Returns number of affected rows if any.

  $affected =
    $jdb->_run_change_loop( 'sql'    => 'UPDATE ...',
                            'values' => [ @bind ]
                           );


On database error, transaction is undone, errors are passed up.

=cut

sub _run_change_loop {

  my ( $self, %arg ) = @_;

  my $sql = $arg{'sql'};

  # If only one level of references given, then take that to mean only
  # 1 row needs to be modified.
  my @val = @{ $arg{'values'} } ;

  # ... so force another level of array reference to avoid special
  # case of loop operation.
  @val = ( [ @val ] )
    unless ref $val[0];


  my $dbh = $self->dbhandle();

  $dbh->begin_work if $self->use_transaction();

  my $noise = JSA::Verbosity->new();
  $noise->make_noise( 2, qq[SQL: $sql] );

  my @affected;
  for my $vals ( @val ) {

    my @bind =  @{ $vals };

    $noise->make_noise( 1, '  '
                            . join ' , ', map { defined $_ ? $_ : 'undef' } @bind
                        );

    my $affected = $self->_run_change_sql->( $sql, @bind );

    push @affected, $affected if $affected;
  }

  $dbh->commit if $self->use_transaction();

  return sum @affected;
}

=head2 INTERNAL METHODS

=back

=over 2

=item B<_run_change_sql>

Executes a given SQL query string with given list of bind values.

Returns number of affected rows if any.

  $affected =
    $jdb->_run_change_sql( 'UPDATE ...', @bind );


On database error, rollbacks the transaction & throws
C<JSA::Error::DBError>.

=cut

sub _run_change_sql {

  my ( $self, $sql, @bind ) = @_;

  my $dbh = $self->dbhandle;

  return $dbh->do( $sql, undef, @bind )
            or do {
                    $dbh->rollback;
                    throw JSA::Error::DBError( $dbh->errstr );
                  };
}

=item B<_check_input>

Checks if input for change operations meet certain criteria.
Currently checks only if a value is present, and if the value is an
array reference if specified.

It takes in a hash of values to be checked and check criteria
identified by key C<_check_>.

Throws C<JSA::Error::BadArgs> if criteria are not met.

  _check_input( 'table' => 'the_table',
                'where' => 'a = ? AND b = ?'.
                'values' => [ ... ],

                '_check_' =>
                  { # these two need to be simply truth
                    # values.
                    'table'  => 0,
                    'where'  => 0,

                    # 'values' value has to be an array
                    # reference.
                    'values' => 1,
                  }
              );

=cut

sub _check_input  {

  my ( %arg ) = @_;

  my %check = %{ delete $arg{'_check_'} };

  my @err;
  for my $c ( keys %check ) {

    my $to_check = $arg{ $c };

    # $to_check is simple scalar.
    unless ( $check{ $c } ) {

      next if $to_check;

      push @err, qq['$c' was not given.];
    }

    next
      if $to_check
      && ref $to_check
      && scalar @{ $to_check };

    push @err, qq['$c' array reference was not given.];
  }

  return unless scalar @err;
  throw JSA::Error::BadArgs( join "\n", @err );
}

sub _name {

  my ( $self ) = @_;

  return $self->{'name'};
}

# Convert scalar to list to string.
sub _where_string {

  my ( $where ) = @_;

  return _to_string( $where, ' AND ' );
}

sub _to_string {

  my ( $in, $sep ) = @_;

  $sep = ', ' unless defined $sep;

  return join $sep, _to_list( $in );
}

sub _to_list {

  my ( $in ) = @_;

  return unless defined $in;

  return ( @{ $in } ) if $in && ref $in;

  return ( $in );
}


1;


__END__

=pod

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

