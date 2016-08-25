package JSA::DB;

=pod

=head1 NAME

JSA::DB - Arranges for database handle given a configuration

=head1 SYNOPSIS

Make an object ...

    $jdb = JSA::DB->new();

Pass jcmt database handle already being used if desired ...

    $jdb->dbhandle( $dbh );

=head1 DESCRIPTION

This package provides a database handle given "ini" style database log in
configuration with "database" section.  See L<OMP::Config> for details.

=cut

use strict; use warnings;

use List::Util qw/sum max/;
use List::MoreUtils qw/any all firstidx/;
use Log::Log4perl;
use Scalar::Util qw/looks_like_number/;

use JSA::Error qw/:try/;
use JSA::DB::Sybase qw/connect_to_db/;
use JSA::LogSetup qw/hashref_to_dumper/;

use OMP::Config;
use OMP::Constants qw /:status/;

$OMP::Config::DEBUG = 0;

my %_config = (
    'db-config' => '/jac_sw/etc/enterdata/enterdata.cfg',

    'name' => __PACKAGE__,

    'dbhandle' => undef,
);

=head1 METHODS

=over 2

=item B<new> constructor

Make a C<JSA::DB> object.  It takes a hash of parameters ...

    db-config - pass file name with database log in information in "ini" format;
                default is /jac_sw/etc/enterdata/enterdata.cfg;

    name      - (optional) pass a string to differentiate db connections
                with same log in configuration; default is 'JSA::DB'.

    $jdb = JSA::DB->new(
        'name'      => 'JSA::DB'
        'db-config' => '/jac_sw/etc/enterdata/enterdata.cfg');

Transactions are set to be used (see C<use_transaction> method).

=cut

sub new {
    my ($class, %arg) = @_;

    my $obj = {};

    foreach my $k (keys %_config) {
        $obj->{$k} = exists $arg{$k} ? $arg{$k} : $_config{$k};
    }

    $obj = bless $obj, $class;
    $obj->use_transaction(1);

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

  my $name = $self->_name() || 'extern-dbh';

  unless (scalar @_) {
      return $self->{ $name }
        if exists $self->{ $name }
        && ref $self->{ $name };

      return connect_to_db($self->{'db-config'}, $name);
  }

  my $log = Log::Log4perl->get_logger('');
  $log->debug('Setting external database handle.');

  $self->{$name} = shift @_;

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

    $jdb->use_transaction(1);

=cut

sub use_transaction {
    my $self = shift @_;

    return $self->{'trans'} unless scalar @_;

    $self->{'trans'} = !! shift @_;

    my $log = Log::Log4perl->get_logger('');
    #$log->trace('use transactions ' . $self->{'trans'} ? 1 : 0);

    return;
}

=item B<select_t>

Executes SELECT query given a hash of table names, array reference of
column names to select, where clauses with placeholders, and array
reference of bind values.

Returns an array reference of hash references of selected columns.

    $result =
      $jdb->select_t('table'   => 'the_table',
                     'columns' => [ 'a', 'b' ],
                     'where'   => [ a = ? AND b = ? ],
                      'values'  => [ 1, 2 ]
                    );

Optional hash pairs are "order" & "group" respectively for C<ORDER BY>
and C<GROUP BY> clauses ...

    $result =
      $jdb->select_t('table'   => 'the_table',
                      ...
                      'order' => [ 'b' , 'a' ]
                    );

On database error, rollbacks the transaction, errors are passed up.

(Note that I<select> is a built in function; "_t" in name refers to
database table.)

=cut

sub select_t {
    my ($self, %arg) = @_;

    _check_input(
        %arg,
        '_check_' => {
            'table'   => 0,
            'columns' => 1,
            'where'   => 0,
            'values'  => 1,
        },
    );

    my $sql = sprintf 'SELECT %s FROM %s WHERE %s',
                      (map {_to_string($_)} @arg{qw/columns table/}),
                      _where_string($arg{'where'});

    foreach my $opt (qw/order group/) {
        next unless exists $arg{ $opt };

        $sql .= sprintf ' %s BY %s', uc $opt, _to_string($arg{$opt});
    }

    return $self->run_select_sql('sql' => $sql,
                                 'values' => $arg{'values'});
}

=item B<select_loop>

It is similar to I<select_t> in terms of parameters accepted and
values returned.  Difference is that instead of selecting everything
in one go ($dbh->selectall_arrayref()), it collects results row by row
($dbh->prepare(), $st->execute(), $st->fetchrow_array*()).

Use it instead when "OR" clauses (by virtue of bind parameter array
references) are in some "large" number (say, 100).  This is mainly to
avoid running in limit of allowed bind parameters; better performance
is a side benefit.

Executes SELECT query given a hash of table name, array reference of
column names to select, where clauses with placeholders, and array
reference of bind values.  In case of bind values are themselves array
references, each array reference is used per SELECT in WHERE clause.

Returns an array reference of hash references of selected columns.

    $result =
      $jdb->select_loop('table'   => 'the_table',
                        'columns' => [ 'a', 'b' ],
                        'where'   => [ a = ? AND b = ? ],
                        'values'  => [[ 1, 2 ], [3, 4]]);

Optional hash pairs are "order" & "group" respectively for C<ORDER BY>
and C<GROUP BY> clauses ...

    $result =
      $jdb->select_loop('table'   => 'the_table',
                         ...
                         'order' => [ 'b' , 'a' ]);

On database related errors, throws L<JSA::Error::DBError> type of
exceptions.

=cut

sub select_loop {
    my ($self, %arg) = @_;

    _check_input(
        %arg,
        '_check_' => {
            'table'   => 0,
            'columns' => 1,
            'where'   => 0,
            'values'  => 1,
        },
    );

    my $sql = sprintf 'SELECT %s FROM %s WHERE %s',
                      (map {_to_string($_)} @arg{qw/columns table/}),
                      _where_string($arg{'where'});

    foreach my $opt (qw/order group/) {
        next unless exists $arg{$opt};

        $sql .= sprintf ' %s BY %s', uc $opt, _to_string($arg{$opt});
    }

    my $log = Log::Log4perl->get_logger('');
    $log->debug(hashref_to_dumper($sql => [@arg{qw/where values/}]));

    my $dbh = $self->dbhandle();

    my $handle_err = sub {
        my ( @text ) = @_;
        my $text = join '', @text;
        $log->error($text);
        throw JSA::Error::DBError $text;
    };

    my $st = $dbh->prepare($sql)
        or $handle_err->('prepare() failed: ', $dbh->errstr(), "\n{ $sql }.");

    my (@out, @bind, @tmp, $tmp, $status);

    foreach my $val (@{$arg{'values'}}) {
        @bind = $val && ref $val ? @{$val} : ($val);

        $status = $st->execute(@bind);

        $handle_err->('execute() failed: ', $dbh->errstr(), "\n{ ", @bind, ' }')
            if $dbh->err();

        $tmp = $st->fetchall_arrayref({});

        $handle_err->('fetchall*() failed: ', $dbh->errstr())
            if $dbh->err();

        next unless $tmp && scalar @{$tmp};

        push @out, @{$tmp};
    }

    return unless scalar @out;

    return $out[0] if 1 == scalar @out
                   && ref $out[0] eq 'ARRAY';

    return [@out];
}

=item B<exist>

A special case of I<select_t>, returns a number to indicate if any
rows exist in given a hash of table names, "WHERE" clauses with
placeholders, and array reference of bind values.

    $count =
      $jdb->exist('table'   => ['transfer t', 'FILES f']
                   'where'  =>
                     q[      t.file_id = f.file_id
                         AND t.file_id like ?
                         AND t.state   = ?
                       ],
                   'values' => ['a20100324%', 't']);

=cut

sub exist {
    my ($self, %arg) = @_;

    my $result = $self->select_t(%arg, 'columns' => ['count(1) AS size'])
        or return 0;

    return $result->[0]{'size'};
}

=item B<run_select_sql>

Returns array reference of hash references (column name is key, column
value is value) given a hash of SQL and bind values.

Returns nothing if nothing is found.

    $result =
      $jdb->run_select_sql('sql' => q[SELECT ... WHERE a = ? AND b = ?],
                           'values' => [1, 2]);

On database error, throws C<JSA::Error::DBError>.

=cut

sub run_select_sql {
    my ($self, %arg) = @_;

    my $dbh = $self->dbhandle;

    my @bind = @{$arg{'values'}};

    my $log = Log::Log4perl->get_logger('');
    $log->trace(hashref_to_dumper('sql' => $arg{'sql'}, 'bind' => $arg{'values'}));

    my $out = $dbh->selectall_arrayref($arg{'sql'}, {'Slice' => {}}, @bind)
        or throw JSA::Error::DBError( $dbh->errstr );

    return unless $out && scalar @{$out};
    return $out;
}

=item B<insert>

Executes INSERT query given a hash of table, array reference of column
names, and an array reference of value array references to be
inserted.

Returns number of affected rows if any.

    $affected =
      $jdb->insert('table'   => 'the_table',
                   'columns' => ['a', 'b'],
                   'values'  => [[1, 2], [4, 6]]);

On database error, rollbacks the transaction, errors are passed up.

=cut

sub insert {
    my ($self, %arg) = @_;

    _check_input(
        %arg,
        '_check_' => {
            'table'   => 0,
            'columns' => 1,
            'values'  => 1,
        },
    );

    my @cols = @{ $arg{'columns'} };
    my $size = scalar @cols;

    my $sql = sprintf 'INSERT INTO %s (%s) VALUES (%s)',
                      $arg{'table'},
                      join(', ', @cols),
                      join(', ', ('?') x $size);

    # Append the string if given, say something like
    # 'WHERE NOT EXIST IN ( SELECT ... )'.
    $sql .= ' ' . $arg{'_append'}
        if $arg{'_append'};

    return $self->_run_change_loop('sql'    => $sql,
                                   'values' => $arg{'values'});
}

=item B<update>

Executes UPDATE query given a hash table name, array reference of SET
clauses, WHERE clause string & and array reference of array references
of values for WHERE clause.

Returns number of affected rows if any.

    # Changes columns 'a' & 'b' of rows
    # where a is 1 & b is 2, or a is 4 & b is 6.
    $affected =
      $jdb->update('table'  => 'the_table',
                   'set'    => ['a = 4', 'b = b +1'],
                   'where'  => 'a = ? AND b = ?',
                   'values' => [[1, 2], [4, 6]]);

On database error, transaction is undone, errors are passed up.

=cut

sub update {
    my ($self, %arg) = @_;

    _check_input(
        %arg,
        '_check_' => {
            'table'  => 0,
            'set'    => 1,
            'where'  => 0,
            'values' => 1,
        },
    );

    my $sql = sprintf 'UPDATE %s SET %s WHERE %s',
                      $arg{'table'},
                      _to_string($arg{'set'}),
                      _where_string($arg{'where'});

    return $self->_run_change_loop('sql'    => $sql,
                                   'values' => $arg{'values'});
}

=item B<delete>

Executes DELETE query given a hash table name, WHERE clause string &
and array reference of array references of values for WHERE clause.

Returns number of affected rows if any.

    # Deletes rows where a is 1, b is 2 OR a is 4 & b is 6.
    $affected =
      $jdb->delete('table'  => 'the_table',
                   'where'  => 'a = ? AND b = ?' ,
                   'values' => [[1, 2], [4, 6]]);

On database error, transaction is undone, errors are passed up.

=cut

sub delete {
    my ($self, %arg) = @_;

    _check_input(
        %arg,
        '_check_' => {
            'table'  => 0,
            'where'  => 0,
            'values' => 1,
        },
    );

    my $sql = sprintf 'DELETE %s WHERE %s',
                      $arg{'table'},
                      _where_string($arg{'where'});

    return $self->_run_change_loop('sql'    => $sql,
                                   'values' => $arg{'values'});
}

=item B<update_or_insert>

Any output of C<update> or C<insert> call back references is returned.
Accepts the following arguments as hash value of ...

  table   - table name in question;

  unique-keys - sub set of column names to search for for UPDATE;

  columns - array reference of column names to be UPDATEd and/or
            INSERTed;

  values  - array reference of array references of values to be
            UPDATEd and/or INSERTed (see update() & insert() method).

  update-only - truth value to skip INSERT even if no rows exist.

    $affected =
      $db->update_or_insert('table' => $name,
                            # For UPDATE.
                            'unique-keys' => ['a', 'b'],
                            # For UPDATE & INSERT.
                            'columns' => ['a', 'b', 'c'],
                            'values'  => [[1, 2, 3], [4, 6, 7]]);


See also I<update()> and I<insert()> methods.

=back

=cut

sub update_or_insert {
    my ($self, %arg) = @_;

    my ($keys, $cols, $vals) = @arg{qw/unique-keys columns values/};

    my %pass = ('columns' => $cols,
                map {$_ => $arg{$_}} qw/table update-only/);

    my @key        = _to_list($keys);
    $pass{'where'} = _where_string([map {" $_ = ? "} @key]);

    # Handle transaction self.  Save old transaction setting to reinstate later.
    my $old_tran = $self->use_transaction();
    $self->use_transaction(0);

    my $dbh = $arg{'dbhandle'};
    $dbh    = $self->dbhandle() unless $dbh;

    my $log = Log::Log4perl->get_logger('');

    my ($key_val, $idx, $set, $rows);

    # Test if a plain array reference is given or an array reference of array
    # references.
    unless (ref $vals->[0]) {
        ($key_val, $idx) = _extract_key_val($cols, $vals, @key);

        $set  = [map {" $cols->[ $_ ] = $vals->[ $_ ] "} @{$idx}];

        _start_trans($dbh);

        $rows = $self->_run_update_or_insert(%pass,
                                             'set'        => $set,
                                             'where-bind' => $key_val,
                                             'values'     => $vals);

        _end_trans($dbh);
    }
    else {
        my $end = scalar @{$vals} - 1;

        foreach my $i (0 .. $end) {
            my $v = $vals->[$i];
            ($key_val, $idx) = _extract_key_val($cols, $v, @key);

            $set = [map {sprintf '%s = %s',
                                 $cols->[ $_ ],
                                 _massage_for_col($v->[$_]) } @{$idx}];

            _start_trans($dbh);

            $rows = $self->_run_update_or_insert(%pass,
                                                 'set'        => $set,
                                                 'where-bind' => $key_val,
                                                 'values'     => $v);

            _end_trans($dbh);
        }
    }

    $self->use_transaction($old_tran);

    return;
}

=head2 INTERNAL METHODS

=over 2

=item B<_run_update_or_insert>

Returns the number of rows affected due to UPDATE or INSERT given a
hash of ...

    table   - name;

    set     - array reference for SET clause for UPDATE query;

    where   - array reference for WHERE clauses with place holders;

    where-bind - array reference of values for place holders in WHERE
                 clause;

    columns - array reference of column names for INSERT query;

    values  - array reference of (array references, each for a new row,
              of) values to be inserted.

Note that C<values> are not used in UPDATE query only in INSERT.
Array reference for SET clause must be have values in place as place
holders are not (yet) supported by L<DBD::Sybase>.

    $rows =
      $self->_run_update_or_insert('table'   => $name,
                                   'set'     => ['a = 1', 'b = 2'],
                                   'where    => ['c = ?'],
                                   'where-bind' => [3],
                                   'columns' => [qw/a b c/],
                                   'values'  => [1, 2, 3]
                                  );

See also I<update()> and I<insert()> methods.

=cut

sub _run_update_or_insert {
    my ($self, %arg) = @_;

    my $dbh = $self->dbhandle();
    my $log = Log::Log4perl->get_logger('');

    my $rows =
      $self->update('values'  => $arg{'where-bind'},
                    map {$_ => $arg{$_}} qw/table set where/);

    my $e = $dbh->err();
    $log->debug('After update, rows: ', $rows, '  err: ', defined $e ? $e : '');

    return if $arg{'update-only'};

    unless ($rows) {
      $rows = $self->insert(map {$_ => $arg{$_}} qw/table columns values/);

      $e = $dbh->err();
      $log->debug('After insert, rows: ', $rows, '  err: ', defined $e ? $e : '');
    }

    return $rows;
}

=item B<_run_change_loop>

Executes table change operations, given a hash of SQL query string and
array reference of array references of bind values (multiple second
level references will affect multiple rows).

Returns number of affected rows if any.

    $affected =
      $jdb->_run_change_loop('sql'    => 'UPDATE ...',
                             'values' => [@bind]);


On database error, transaction is undone, errors are passed up.

=cut

sub _run_change_loop {
    my ($self, %arg) = @_;

    my $sql = $arg{'sql'};

    # If only one level of references given, then take that to mean only
    # 1 row needs to be modified.
    my @val = @{$arg{'values'}};

    # ... so force another level of array reference to avoid special
    # case of loop operation.
    @val = ([@val])
        unless ref $val[0];

    my $dbh = $self->dbhandle();

    $dbh->begin_work if $self->use_transaction();

    my @affected;
    for my $vals (@val) {
        my @bind =  @{ $vals };

        my $affected = $self->_run_change_sql($sql, @bind);

        push @affected, $affected if $affected;
    }

    $dbh->commit if $self->use_transaction();

    return sum @affected;
}

=item B<_run_change_sql>

Executes a given SQL query string with given list of bind values.

Returns number of affected rows if any.

    $affected =
      $jdb->_run_change_sql('UPDATE ...', @bind);


On database error, rollbacks the transaction & throws
C<JSA::Error::DBError>.

=cut

sub _run_change_sql {
    my ($self, $sql, @bind) = @_;

    my $log = Log::Log4perl->get_logger('');
    $log->trace(hashref_to_dumper('sql' => $sql , 'bind' => \@bind));

    my $dbh = $self->dbhandle;

    return $dbh->do($sql, undef, @bind)
        or do {
            $dbh->rollback() if $self->use_transaction();
            $log->error('rollback; ' , $dbh->errstr);
            throw JSA::Error::DBError( $dbh->errstr );
        };
}

{
    my $trans = 0;

=item B<_start_trans>

Given a database handle, starts a transaction; returns nothing.

    _start_trans($dbh);

=cut

    sub _start_trans {
        my ($dbh) = @_;

        $dbh->{'AutoCommit'} and return;

        my $log = Log::Log4perl->get_logger('');
        $log->trace('Starting transaction');

        # Start transaction.
        if ($trans == 0) {
          $dbh->{'AutoCommit'} = 0;
          #$dbh->begin_work();
        }

        $trans++;
        return;
    }

=item B<_end_trans>

Given a database handle, commits a transaction if there were no
errors.  On error, rolls back a transaction.  Returns nothing.

    _end_trans($dbh);

=cut

    sub _end_trans {
        return unless $trans;

        my ($dbh) = @_;

        # AutoCommit need to be false already to be able to use explicit transactions
        # and avoid DBI errors related to AutoCommit being already on & similar.
        $dbh->{'AutoCommit'} and return;

        my $autoc = $dbh->{'AutoCommit'};

        my $log = Log::Log4perl->get_logger('');

        unless ($dbh->err()) {
          $log->trace('Commiting transaction');

          if ($trans == 1  && ! $autoc) {
              $dbh->commit()
                  or throw JSA::Error 'Cannot commit transaction: ' . $dbh->errstr();
          }

          -- $trans;
          return;
        }

        $log->error('Rolling back transaction: ', $dbh->errstr());

        if ($trans && ! $autoc) {
            $trans = 0;
            $dbh->rollback()
                or throw JSA::Error 'Cannot rollback transaction: ' . $dbh->errstr();
        }
        return;
    }
}

=item B<_extract_key_val>

Returns a list of two array references -- one of (database table) key
values, other of indexen which do not relate to keys -- given an array
reference of table column names, an array reference of related values,
and a list of key names.

    ($keyvals, $indices) = _extract_key_val($cols, $vals, @key);

=cut

sub _extract_key_val {
    my ($cols, $vals, @key) = @_;

    return unless scalar @key;

    my $end = scalar @{$cols} - 1;

    my (@idx, @keyidx, @keyval);

    foreach my $i (0 .. $end) {
        if (any {$cols->[$i] eq $_} @key) {
            push @keyidx, $i;
            push @keyval, $vals->[$i];
        }
        else {
            push @idx, $i;
        }
    }

    return unless scalar @keyidx;

    return ([@keyval], [@idx]);
}

=item B<_check_input>

Checks if input for change operations meet certain criteria.
Currently checks only if a value is present, and if the value is an
array reference if specified.

It takes in a hash of values to be checked and check criteria
identified by key C<_check_>.

Throws C<JSA::Error::BadArgs> if criteria are not met.

    _check_input('table' => 'the_table',
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
    my (%arg) = @_;

    my %check = %{delete $arg{'_check_'}};

    my @err;
    foreach my $c (keys %check) {
        my $to_check = $arg{$c};

        # $to_check is simple scalar.
        unless ($check{$c}) {
            next if $to_check;

            push @err, "'$c' was not given.";
        }

        next if $to_check
             && ref $to_check
             && scalar @{$to_check};

        push @err, "'$c' array reference was not given.";
    }

    return unless scalar @err;

    my $log = Log::Log4perl->get_logger('');
    $log->trace(hashref_to_dumper('input error' => \@err));

    throw JSA::Error::BadArgs(join "\n", @err);
}

=item B<_name>

Returns the name (string) used to associate with a database
connection.

    $net_name = $jdb->_name();

=cut

sub _name {
    my ($self) = @_;

    return $self->{'name'};
}

=item B<_where_string>

Returns a string for WHERE clause given a plain scalar or an array
reference of strings.  Multiple strings are joined by "AND".

    $sql_where = _where_string($list);

=cut

sub _where_string {
    my ($where) = @_;

    return _to_string($where, ' AND ');
}

=item B<_to_string>

Returns a string joined by a separator given a plain scalar or an
array reference.  Default separator is comma.

    $string = _to_string( $list, ':' );

=cut

sub _to_string {
    my ($in, $sep) = @_;

    $sep = ', ' unless defined $sep;

    return join $sep, _to_list($in);
}

=item B<_to_list>

Returns a list given a plain scalar or an array reference.

    @expanded = _to_list($list);

=cut

sub _to_list {
    my ($in) = @_;

    return unless defined $in;

    return (@{$in}) if $in && ref $in;

    return ($in);
}

sub _simplify_arrayref {
    my ($self, $in) = @_;

    return
        unless $in && scalar @{$in};

    return
        [map {$_->[0]} @{$in}];
}

sub _simplify_arrayref_hashrefs {
    my ($self, $in) = @_;

    return $in
        unless $in
            && ref $in
            && scalar @{$in};

    my $ele = $in->[0];

    return $in
        unless defined $ele
            && ref $ele eq 'HASH';

    my @key = keys %{$ele};

    return $in
      if 1 != scalar @key;

    return
      [map {$_->{$key[0]}} @{$in}];
}

sub _massage_for_col {
    my @in = @_;

    for (@in) {
        $_ =  ! defined $_
              ? 'NULL'
              : looks_like_number($_)
                ? $_
                : "'$_'";
    }

    return @in;
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
