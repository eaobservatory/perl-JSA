package JSA::DB::TableTransfer;

=pod

=head1 NAME

JSA::DB::TableTransfer - Check file status, transfer files to CADC

=head1 SYNOPSIS

Make an object ...

    $xfer = new JSA::DB::TableTransfer(db => new JSA::DB());

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
use File::Basename qw/fileparse/;
use DateTime;
use DateTime::Duration;
use Pod::Usage;
use List::Util qw/min sum/;
use List::MoreUtils qw/any/;
use Log::Log4perl;

use JSA::Error qw/:try/;

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

    db           - JSA::DB object, which has succesfully connected to database;
    transactions - (optional) truth value, used when changing tables;

    $xfer = new JSA::DB::TableTransfer(db => $jsa_db);

Throws L<JSA::Error::BadArgs> error when database handle is invalid object.

=cut

sub new {
    my ($class, %arg) = @_;

    my $db = $arg{'db'};
    throw JSA::Error::BadArgs "JSA database object is invalid."
        unless defined $db
            && ref $db;

    my $obj = {};

    foreach (qw/db transactions/) {
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

    $xfer->add_found(files => [@files]);

=cut

sub add_found {
    my ($self, %args) = @_;

    my ($files, $dry_run) = map {$args{$_}} qw/files dry_run/;

    return unless scalar @$files;

    my $state = $_state{'found'};
    my @values = map {
        my $alt = _fix_file_name((fileparse($_, ''))[0]);
        [$alt, $state, $alt eq $_ ? undef : $_];
    } @$files;

    my $db = $self->_jdb();

    return $db->insert('table'   => $_state_table,
                       'columns' => ['file_id', 'status', 'location'],
                       'values'  => \@values,
                       'on_duplicate' => 'status=status', # Should do nothing
                       'dry_run' => $dry_run,
    );
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
              WHEN SUBSTRING( file_id, 1, 4 ) = 'rxh3'
                -- RxH3 files.
                THEN SUBSTRING( file_id, 6, 8 )
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

=item B<unique_keys>

Returns list of columns to uniquely identify a row.

    @keys = JSA::DB::TableTransfer->unique_keys();

=cut

sub unique_keys {
  return qw/file_id/;
}

=item B<_dbhandle>

Returns the database handle, which is obtained from our JSA::DB
object.

    $dbh = $xfer->_dbhandle();

=cut

sub _dbhandle {
    my $self = shift @_;

    return $self->_jdb()->dbhandle();
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

    my $db = $self->_jdb();
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

      # Don't try to append .sdf if it already ends in .fits.
      return $file if $file =~ /\.fits/;

      my $suffix   = '.sdf';
      $suffix_re ||= qr/$_$/ for quotemeta $suffix;

      return $file =~ $suffix_re
                    ? $file
                    : "${file}${suffix}";
  }
}

=item B<put_state>

    $xfer->put_state(state => $state, files => [...], comment => '...');

=cut

sub put_state {
    my ($self, %args) = @_;

    my ($files, $state, $dry_run) = map {$args{$_}} qw/files state dry_run/;

    _check_state( $state );

    (my $state_col, $state) = _alt_state($_state{$state});

    my $db = $self->_jdb();

    my @alt = map {_fix_file_name($_)} sort @{$files};

    # Ensure comment is not too long to store in the database, e.g. if it
    # was captured from an error message.
    my $comment = (defined $args{'comment'})
        ? (substr $args{'comment'}, 0, 240)
        : undef;

    $db->update_or_insert(
        table   => $_state_table,
        keys    => ['file_id'],
        columns => ['file_id', $state_col, 'comment'],
        values  => [map {[$_, $state, $comment]} @alt],
        dry_run => $dry_run);
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

sub _jdb {
    my ($self) = @_;

    return $self->{'db'};
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
