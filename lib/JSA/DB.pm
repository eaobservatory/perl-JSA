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

use Data::Dumper;
use List::Util qw/sum max/;
use List::MoreUtils qw/any all firstidx/;
use Log::Log4perl;
use Scalar::Util qw/looks_like_number/;

use JSA::Error qw/:try/;
use JSA::DB::MySQL qw/connect_to_db/;
use JSA::LogSetup qw/hashref_to_dumper/;

use OMP::Config;
use OMP::Constants qw /:status/;

$OMP::Config::DEBUG = 0;

my %_config = (
    'db-config' => '/jac_sw/etc/enterdata/enterdata.cfg',

    'name' => __PACKAGE__,
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

    my $obj = {_dbh => {}};

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

Uses the "name" to differentiate one data instance from another, re-using
a cached handle if one has already been created for the current "name".

Override internal database handle to jcmt database ...

    $jdb->dbhandle($dbh);

Passes up any errors thrown.

Database handles are released at the end of the program via C<END>.

=cut

{
    my %_handles;

    sub dbhandle {
        my $self = shift @_;

        my $name = $self->_name() || 'extern-dbh';

        my $log = Log::Log4perl->get_logger('');

        # If we have been given a handle, store it and return.
        # (And don't register it in %_handles, meaning we will not
        # automatically disconnect in END.)
        if (scalar @_) {
            my $dbh = shift;

            $log->debug('Setting external database handle.');

            $self->{'_dbh'}->{$name} = $dbh;

            return;
        }

        # Check for a handle by name in this instance.
        if (exists $self->{'_dbh'}->{$name}) {
            my $dbh = $self->{'_dbh'}->{$name};

            return $dbh if $dbh->ping();
        }

        # Check for handle in the class-level cache by "key", which is
        # made up of the name and configuration file.  Do this to
        # replicate the old module-level caching previously provided by
        # the JSA::DB::MySQL module.
        my $config = $self->{'db-config'};
        my $key = join(':', $name, $config);
        my $dbh = undef;

        if (exists $_handles{$key}) {
            $dbh = $_handles{$key} if $_handles{$key}->ping();
        }

        unless (defined $dbh) {
            # Make a new connection.
            $dbh = connect_to_db($config);

            # Store in the class-level cache.
            $_handles{$key} = $dbh;
        }

        # Store in the instance cache.
        $self->{'_dbh'}->{$name} = $dbh;

        return $dbh;
    }

    sub _release_dbh {
        my $log = Log::Log4perl->get_logger('');

        foreach my $k (keys %_handles) {
            my $v = $_handles{$k};

            if ($v) {
                $v->disconnect()
                    or $log->warn("Problem disconnecting from $k: " , $v->errstr());

                undef $v;
            }
        }

        return
    }
}

END {_release_dbh();}

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

=item B<select_loop>

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
                   'values'  => [[1, 2], [4, 6]],
                   'on_duplicate' => 'col="val"',
                   'dry_run' => $dry_run);

On database error, rollbacks the transaction, errors are passed up.

=cut

sub insert {
    my ($self, %arg) = @_;

    my $log = Log::Log4perl->get_logger('');

    _check_input(
        %arg,
        '_check_' => {
            'table'   => 0,
            'columns' => 1,
            'values'  => 1,
        },
    );

    my ($table, $cols, $dry_run, $values) = @arg{qw/table columns dry_run values/};

    my $sql = sprintf(
        'INSERT INTO %s (%s) VALUES (%s)',
        $table,
        join(', ', @$cols),
        join(', ', map {'?'} @$cols));

    $sql .= ' ON DUPLICATE KEY UPDATE ' . $arg{'on_duplicate'}
        if exists $arg{'on_duplicate'};

    if ($dry_run) {
        $log->debug(
            'Dry-run: would have done insert: ' . $sql .
            ' with values: ' . Dumper($values));

        return 0;
    }

    return $self->_run_change_loop('sql'    => $sql,
                                   'values' => $values);
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

Accepts the following arguments:

    table   - table name in question;

    keys    - sub set of column names to search for for UPDATE;

    columns - array reference of column names to be UPDATEd and/or
              INSERTed;

    values  - array reference of array references of values to be
              UPDATEd and/or INSERTed (see update() & insert() method).

    $db->update_or_insert(table   => $name,
                          # For UPDATE.
                          keys    => ['a', 'b'],
                          # For UPDATE & INSERT.
                          columns => ['a', 'b', 'c'],
                          values  => [[1, 2, 3], [4, 6, 7]]);

Note: this method uses an "INSERT ... ON DUPLICATE KEY UPDATE ..." statement,
so it should not be used on tables with multiple unique indexes.  (See the
MySQL documentation for this statement for details.)

=back

=cut

sub update_or_insert {
    my ($self, %arg) = @_;

    my ($table, $keys, $cols, $vals, $dry_run) = @arg{
        qw/table keys columns values dry_run/};

    my $where = _where_string([map {" $_ = ? "} @$keys]);

    # Handle transaction self.  Save old transaction setting to reinstate later.
    my $old_tran = $self->use_transaction();
    $self->use_transaction(0);

    my $dbh = $self->dbhandle();

    my $log = Log::Log4perl->get_logger('');

    my %keys = map {$_ => 1} @$keys;
    my @non_key_cols = grep {not exists $keys{$_}} @$cols;

    my $sql = sprintf(
        'INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s',
        $table,
        join(', ', @$cols),
        join(', ', map {'?'} @$cols),
        join(', ', map {$_ . ' = VALUES(' . $_ . ')'} @non_key_cols),
    );

    $log->debug('Insert/update query: ' . $sql);

    foreach my $v (@$vals) {
        if ($dry_run) {
            $log->debug('Dry-run: would have inserted/updated values: ' . Dumper($v));
            next;
        }

        _start_trans($dbh);

        my $rows = $self->_run_change_sql($sql, @$v);

        my $e = $dbh->err();
        $log->debug('After insert/update, rows: ', $rows, '  err: ', defined $e ? $e : '');

        _end_trans($dbh);
    }

    $self->use_transaction($old_tran);

    return;
}

=head2 INTERNAL METHODS

=over 2

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

    return '' unless defined $in;
    return $in unless ref $in;

    return join $sep, @$in;
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
