package JSA::DB::TableTransfer;

=pod

=head1 NAME

JSA::DB::TableTransfer - Check file status, transfer files to CADC

=head1 SYNOPSIS

Make an object ...

    $xfer = new JSA::DB::TableTransfer('dbhandle' => $dbh);

Set copied state for some files ...

    $xfer->put_state(
        state => 'copied',
        files => ['a20100612_00005_01_0001.sdf',
                  'a20100612_00006_01_0001.sdf']);

Find copied files ...

    $xfer->get_files(state => 'copied', date => 20100612);

=head1 DESCRIPTION

This package manipulates the database table to track progress of raw file data
ingestion and transfer to CADC.

The file ingestion process adds a row on successful ingestion; file copy
to CADC processes update the state. Disk cleaning process should delete
the rows (to be implemented).

=cut

use strict; use warnings;

use File::Spec;
use DateTime;
use DateTime::Duration;
use Pod::Usage;
use Getopt::Long qw/:config gnu_compat no_ignore_case no_debug/;
use List::Util qw/min sum/;
use List::MoreUtils qw/any/;
use Log::Log4perl;

use JSA::Error qw/:try/;
use OMP::Config;

$OMP::Config::DEBUG = 0;

my $_state_table = 'transfer';

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
    transferred
    simulation
    final

=over 2

=item B<new> constructor

Make a C<JSA::DB::TableTransfer> object.  It takes a hash of parameters ...

    dbhandle     - JSA::DB object, which has succesfully connected to database;
    transactions - (optional) truth value, used when changing tables;

    $xfer = new JSA::DB::TableTransfer('dbhandle' => $dbh);

Throws L<JSA::Error::BadArgs> error when database handle is invalid object.

=cut

sub new {
    my ($class, %arg) = @_;

    my $dbh = $arg{'dbhandle'};
    throw JSA::Error::BadArgs "Database handle object is invalid."
        unless defined $dbh
            && ref $dbh;

    my $obj = {};

    foreach (qw/dbhandle transactions/) {
        next unless exists $arg{$_};

        $obj->{$_} = $arg{$_};
    }

    $obj = bless $obj, $class;

    return $obj;
}

our %_state = (
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

    #  File has been put in CADC transfer directory.
    'copied' => 'c',

    #  File has been put in intermediate directory.
    'copied_pre_cadc' => 'p',

    #  File present in CADC directory.
    'transferred' => 't',

    'final' => 'z',
);

my %_rev_state = map {$_state{$_} => $_} keys %_state;

=item B<get_found_files>

Return a array reference of files with C<found> state, given a partial file
name.

    $files = $xfer->get_found_files(20100612);

=cut

sub get_found_files {
    my ($self, $pattern) = @_;

    my $sql = sprintf
        " SELECT s.file_id , s.location
            FROM $_state_table s
            WHERE s.status = ?
              AND s.location IS NOT NULL";

    $sql .= ' AND file_id like ? '
        if $pattern;

    $sql .= ' ORDER BY s.file_id, s.location ';

    my $dbh = $self->_dbhandle();
    my $out = $dbh->selectcol_arrayref($sql,
                                       # Return only file paths.
                                       {'Columns' => [2]},
                                       $_state{'found'},
                                       ($pattern ? $pattern : ()))
        or throw JSA::Error::DBError $dbh->errstr;


    return $out;
}

=item B<add_found>

Adds state of C<found> of given array reference of files (absolute
paths).

    $xfer->add_found([@files]);

=cut

sub add_found {
    my ($self, $files) = @_;

    my $vals = _process_paths($files)
        or return;

    my $db = $self->_make_jdb();

    return $db->insert('table'   => $_state_table,
                       'columns' => ['file_id', 'status', 'location'],
                       'values'  => $vals);
}

=item B<set_found>

Updates state of C<found> of given array reference of files (absolute
paths).

    $xfer->set_found([@files]);

=cut

sub set_found {
    my ($self, $files) = @_;

    return $self->put_found($files, my $update_only = 1);
}

=item B<put_found>

Adds or changes state of C<found> of given array reference of files
(absolute paths).

    $xfer->put_found([@files]);

=cut

sub put_found {
    my ($self, $files, $mod_only) = @_;

    my $vals = _process_paths($files)
        or return;

    my $db = $self->_make_jdb();

    return $db->update_or_insert('table'       => $_state_table,
                                 'unique-keys' => ['file_id'],
                                 'columns'     => ['file_id', 'status', 'location'],
                                 'values'      => $vals,
                                 'update-only' => $mod_only);
}

sub _process_paths {
    my ($paths) = @_;

    my @path = @{$paths};

    return unless scalar @path;

    my $state = $_state{'found'};

    require File::Basename;
    import File::Basename qw/fileparse/;

    my @val;
    for my $f (@path) {
        my $alt = _fix_file_name((fileparse($f, ''))[0]);
        push @val, [$alt, $state, $alt eq $f ? undef : $f];
    }

    return unless scalar @val;
    return [@val];
}

=item B<code_to_descr>

Returns a descriptive text given a state code.

    $text = JSA::DB::TableTransfer::code_to_descr('f');

=cut

sub code_to_descr {
    my ($code) = @_;

    return unless exists $_rev_state{$code};
    return $_rev_state{$code};
}

=item B<descr_to_code>

Returns a state code given known descriptive text.

    $code = JSA::DB::TableTransfer::descr_to_code('found');

=cut

sub descr_to_code {
    my ($descr) = @_;

    return unless exists $_state{$descr};
    return $_state{$descr};
}

=item I<get_files_not_end_state>

Returns a list of files not in transferred state at CADC.

    $all = $xfer->get_files_not_end_state();

It takes an optional file name SQL pattern to return only the matching
files.

    $jun12 = $xfer->get_files_not_end_state('%20100612%');

=cut

sub get_files_not_end_state {
    my ($self, $pattern) = @_;

    my @state = @_state{qw/deleted
                           transferred
                           simulation
                           ignored
                          /};

    my $sql = sprintf
        "SELECT s.file_id, s.status, s.error, s.comment,
           -- Extract date from file name.
           CASE
              WHEN SUBSTRING( file_id, 1, 1 ) = 's'
                -- SCUBA-2 files.
                THEN SUBSTRING( file_id, 4, 8 )
             ELSE
                -- ACSIS files.
                SUBSTRING( file_id, 2, 8 )
            END AS date
            FROM $_state_table s
            WHERE (  s.status NOT IN ( %s )
                  OR s.error = 1
                  )",
        join ', ', ('?') x scalar @state;

    $sql .= ' AND file_id like ? '
        if $pattern;

    $sql .= ' ORDER BY date, file_id';

    my $dbh = $self->_dbhandle();
    my $out = $dbh->selectall_hashref($sql, 'file_id', undef,
                                      @state,
                                      ($pattern ? $pattern : ()))
        or throw JSA::Error::DBError $dbh->errstr;

    return unless $out && keys %{$out};

    return $out;
}

=item B<mark_transferred_as_deleted>

I<Marks> given list of files as I<deleted> which have been already marked as
being transferred.

    $xfer->mark_transferred_as_deleted([@file]);

=cut

sub mark_transferred_as_deleted {
    my ($self, $files) = @_;

    my $sql = sprintf
        "UPDATE %s SET status = '%s' WHERE status = ? AND file_id IN (%s)",
        $_state_table,
        $_state{'deleted'},
        join ', ', ('?') x scalar @{$files};

    # Use the same $dbh during a transaction.
    my $dbh = $self->_dbhandle();

    my $log = Log::Log4perl->get_logger('');
    $log->info('Before marking files as deleted');

    $dbh->begin_work if $self->_use_trans();

    my @file = sort @{$files};
    my @alt  = map {_fix_file_name($_)} @file;

    $log->debug(join("  \n", @alt));

    my $affected = $self->_run_change_sql($sql, $_state{'transferred'}, @file);

    $dbh->commit if $self->_use_trans();

    return $affected;
}

=item B<name>

Returns the name of the table in which to collect, change states for
raw files.

    $name = JSA::DB::TableTransfer->name();

=cut

sub name {
    return $_state_table;
}

=item B<unique_keys>

Returns list of columns to uniquely identify a row.

    @keys = JSA::DB::TableTransfer->unique_keys();

=cut

sub unique_keys {
  return qw/file_id/;
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

=item B<get_files>

Return a array reference of files in the given state.

    $files = $xfer->get_files(state => $state, %filter);

=cut

sub get_files {
    my ($self, %filter) = @_;

    my $log = Log::Log4perl->get_logger('');

    my ($state, $date, $instr) = _extract_filter(%filter);
    my $jac = $filter{'keep_jac'};

    my @select = qw/file_id comment/;
    push @select, 'location' if $state eq 'found';

    (my $state_col, $state) = _alt_state($_state{$state});

    my %where;
    $where{" $state_col = ?"} = $state;

    my $fragment = sprintf '%s%%', join '%', grep {$_} $instr, $date;
    $where{' file_id like ?'} = $fragment if defined $fragment;

    $where{' keep_jac = ? '} = $jac if defined $jac;

    $log->info("Getting files from JAC database with state '${state}'");

    my $db = $self->_make_jdb();
    my $out = $db->select_loop('table'   => $_state_table,
                               'columns' => [@select],
                               'where'   => [keys %where],
                               'values'  => [[values %where]]);

    return unless $out
               && ref $out && scalar @{$out};

    return $db->_simplify_arrayref_hashrefs($out);
}

sub _extract_filter {
    my (%filter) = @_;

    my ($state, $date, $instr) = @filter{qw/state date instrument/};

    _check_state($state);
    _check_filename_part($date) if $date;

    $instr = _translate_instrument($instr);

    return ($state, $date, $instr);
}

sub _translate_instrument {
    my ($instr) = @_;

    return '' unless $instr;
    return 's' if $instr =~ /^scuba-?2\b/i;
    return 'a' if $instr =~ /^acsis\b/i;

    return lc "s${1}" if $instr =~ /^s?([48][a-d])\b/i;

    throw JSA::Error::BadArgs "Unknown instrument, '$instr', given.";
}

sub _check_filename_part {
    my ($part) = @_;

    my $parse_date =
        qr/ ^
            2\d{3}
            (?: 0[1-9] | 1[0-2] )
            (?: 0[1-9] | [12][0-9] | 3[01] )
            $
          /x;

    return if $part
           && length $part > 1
           && (# Assume partial file name to be a date if it is a 8-digit number.
               $part =~ m/^\d{8}/
                    ? $part =~ $parse_date
                    : 1);

    throw JSA::Error::BadArgs 'No valid date given to check for files.';
}

{
  my $suffix_re;

  sub _fix_file_name {
      my ($file) = @_;

      my $suffix   = '.sdf';
      $suffix_re ||= qr/$_$/ for quotemeta $suffix;

      return $file =~ $suffix_re
                    ? $file
                    : "${file}${suffix}";
  }
}

sub _run_select_sql {
    my ($self, $dbh, %bind) = @_;

    my ($file, $state) = @bind{qw/file state/};

    my $sql = "SELECT file_id from $_state_table";

    if (any {$_} ($file, $state)) {
        (my $state_col, undef) = _alt_state($state);

        my @where;
        push @where, ' file_id like ?' if $file;
        push @where, " $state_col = ?" if defined $state;

        $sql .= ' WHERE ' . join ' AND ', @where;
    }

    $sql .= ' ORDER BY file_id';

    my $out = $dbh->selectall_arrayref($sql, undef, $file, $state)
        or throw JSA::Error::DBError $dbh->errstr;

    return unless $out && scalar @{$out};

    require JSA::DB;
    return JSA::DB->_simplify_arrayref($out);
}


=item B<put_state>

    $xfer->put_state(state => $state, files => [...], comment => '...');

=cut

sub put_state {
    my ($self, %args) = @_;

    my ($files, $state) = map {$args{$_}} qw/files state/;

    _check_state( $state );

    (my $state_col, $state) = _alt_state($_state{$state});

    my $db = $self->_make_jdb();

    my @alt = map {_fix_file_name($_)} sort @{$files};

    return $db->update_or_insert(
        'table'         => $self->name(),
        'unique-keys'   => ['file_id'],
        'columns'       => ['file_id', $state_col, 'comment'],
        'values'        => [map {[$_, $state, $args{'comment'}]} @alt],
        'dbhandle'      => $self->_dbhandle());
}

sub _run_change_sql {
    my ($self, $sql, @bind) = @_;

    my $dbh = $self->_dbhandle();

    return $dbh->do($sql, undef, @bind)
        or do {
            $dbh->rollback if $self->_use_trans();
            throw JSA::Error::DBError $dbh->errstr;
        };
}

sub _check_hashref {
    my ($h) = @_;

    return $h && ref $h && keys %{$h};
}

sub _check_state {
  my ($type) = @_;

  return if exists $_state{$type};

  throw JSA::Error::BadArgs
      sprintf "Unknown state type, %s, given, exiting ...\n",
              (defined $type ? $type : 'undef');
}

sub _alt_state {
    my ($state) = @_;

    my $re = qr/^ e (?:rr (?:or)? )? $/x;

    unless ($state =~ $re) {
        return ('status' => $state);
    }

    return ('error' => 1);
}

sub _make_jdb {
    my ($self) = @_;

    require JSA::DB;

    my $dbh = $self->_dbhandle();
    return JSA::DB->new('name' => 'transfer-change-add',
                        ($dbh && ref $dbh
                            ? ('dbhandle' => $dbh)
                            : ()));
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
