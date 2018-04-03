package JSA::EnterData;

=head1 NAME

JSA::EnterData - Parse headers and store in database

=head1 SYNOPSIS

    # Create new object, with specific header dictionary.
    my $enter = new JSA::EnterData::SCUBA2('dict' => '/path/to/dict');

    # Upload metadata for Jun 25, 2008.
    $enter->prepare_and_insert(date => '20080625');

=head1 DESCRIPTION

JSA::EnterData is a object oriented module to enter observation
metadata into the JCMT database.  It is the base class for a number of
instrument-specific subclasses and as such should generally not be used
directly -- instead instances of the subclasses should be constructed.

Reads the headers of all data from either the current date or the
specified UT date, and uploads the results to the header database. If
no date is supplied the current localtime is used to determine the
relevant UT date (which means that it will still pick up last night's
data even if run after 2pm).

=cut

use strict;
use warnings;

use Data::Dumper;
use File::Temp;
use List::MoreUtils qw/any all/;
use List::Util qw/min max/;
use Log::Log4perl;
use Scalar::Util qw/blessed looks_like_number/;

use Astro::Coords::Angle::Hour;

use JSA::DB;
use JSA::Headers qw/read_jcmtstate read_wcs/;
use JSA::Datetime qw/make_datetime/;
use JSA::DB::TableCOMMON;
use JSA::Starlink qw/try_star_command/;
use JSA::Error qw/:try/;
use JSA::Files qw/looks_like_rawfile/;
use JSA::WriteList qw/write_list/;
use JSA::DB::TableTransfer;
use JCMT::DataVerify;

use OMP::ArchiveDB;
use OMP::DBbackend::Archive;
use OMP::Info::ObsGroup;
use OMP::DateTools;
use OMP::General;
use OMP::FileUtils;
use OMP::Info::Obs;


use DateTime;
use DateTime::Format::ISO8601;
use Time::Piece;

use NDF;

$| = 1; # Make unbuffered

=head2 METHODS

=over 2

=item B<new>

Constructor.  A data dictionary file name is required.

  $enter = JSA::EnterData->new('dict' => '/file/path/');

Configuration values which can be passed as key-value pairs are:

=over 4

=item I<dict> C<file name>

File name for data dictionary.

=back

=cut

sub new {
    my ($class, %args) = @_;

    my $dict = $args{'dict'};
    throw JSA::Error::FatalError('No valid data dictionary given')
        unless defined $dict ;
    throw JSA::Error::FatalError("Data dictionary, $dict, is not a readable file.")
        unless -f $dict && -r _;

    my $obj = bless {
        dictionary => $class->create_dictionary($dict),

        # To keep track of already processed files.
        _cache_old_date => undef,
        _cache_touched => {},
    }, $class;

    return $obj;
}

=item B<force_db>

Returns the truth value, when called without arguments, to indicate
whether searching the database for data is forced.

    $db = $enter->force_db;

Else, sets the given truth value; returns nothing.  When the value is
true, I<force-disk> is marked false (see I<new>).

    $enter->force_db( 0 );

=cut

sub force_db {
    my $self = shift;

    return
        ! ($OMP::ArchiveDB::FallbackToFiles && $OMP::ArchiveDB::SkipDBLookup)
        unless scalar @_;

    my ($force) = @_;

    $OMP::ArchiveDB::FallbackToFiles =
    $OMP::ArchiveDB::SkipDBLookup = ! $force;

    return;
}

=item B<force_disk>

Returns the truth value, when called without arguments, to indicate
whether searching the disk for data is forced.

    $disk = $enter->force_disk;

Else, sets the given truth value; returns nothing.  When the value is
true, I<force-db> is marked false (see I<new>).

    $enter->force_disk(1);

=cut

sub force_disk {

  my $self = shift;

  return
      $OMP::ArchiveDB::FallbackToFiles && $OMP::ArchiveDB::SkipDBLookup
      unless scalar @_;

  my ($force) = @_;

  # Force observation queries to query files on disk rather than the database.
  $OMP::ArchiveDB::FallbackToFiles =
  $OMP::ArchiveDB::SkipDBLookup = !! $force;

  return;
}

=item B<get_dictionary>

Returns the data dictionary.

    $dictionary = $enter->get_dictionary();

=cut

sub get_dictionary {
    my ($self) = @_;
    return $self->{'dictionary'};
}

=item B<prepare_and_insert>

Inserts observation in database retrieved from disk (see also
I<insert_observations> method) for a date or given list of files.

The current date is used if no date has been explicitly set.

    # Insert either for the set or current date; disk is searched.
    $enter->prepare_and_insert();

    # Insert for Jun 25, 2008; ignores given files if any; disk is
    # searched.
    $enter->prepare_and_insert(date => '20080625');

    # Insert for only given files; ignores date; disk is not searched as
    # there is no reason to.
    $enter->prepare_and_insert(files => \@files);

Options:

=over 4

=item calc_radec

Calculate observation bounds.

=item dry_run

Do not write to the database.

=item process_simulation

Include simulation observations.

=item skip_state

Do not set file state in transfer table.

=item update_only_inbeam

Update only the C<INBEAM> header value.

=item update_only_obstime

Update only the times for an observation.

=back

=cut

sub prepare_and_insert {
    my ($self, %arg) = @_;

    my $log = Log::Log4perl->get_logger('');

    my $dry_run = $arg{'dry_run'};
    my $skip_state = $arg{'skip_state'};

    my %update_args = map {$_ => $arg{$_}} qw/
        calc_radec
        process_simulation
        update_only_inbeam update_only_obstime/;

    my %obs_args = ();

    my $date = undef;
    if (exists $arg{'date'}) {
        $date = _reformat_datetime($arg{'date'});
        $obs_args{'date'} = $date;
    }
    else {
        # Set up $date just for usage in managing the touched files cache.
        $date = $date = DateTime->now(time_zone => 'UTC')->ymd('');
    }

    if (exists $arg{'files'}) {
        $obs_args{'files'} = _unique_files($arg{'files'});
    }

    if (defined $self->{'_cache_old_date'} && $date ne $self->{'_cache_old_date'}) {
        $log->debug("clearing file cache");

        $self->{'_cache_touched'} = {};
    }

    $self->{'_cache_old_date'} = $date;

    my $name = $self->instrument_name();

    # Retrieve observations from disk.  An Info::Obs object will be returned
    # for each subscan in the observation.  No need to retrieve associated
    # obslog comments. That's <no. of subsystems used> *
    # <no. of subscans objects returned per observation>.
    my $group = $self->_get_obs_group(
        dry_run => $dry_run,
        skip_state => $skip_state,
        %obs_args);

    return unless $group && ref $group;

    # Note: this was previously SCUBA-2 specific, but can probably
    # be called harmlessly for ACSIS since it doesn't have flatfields.
    my @obs = $self->_filter_header(
        [$group->obs],
        'OBS_TYPE' => [qw/FLATFIELD/],
    );

    $log->debug(
        (exists $obs_args{'date'})
        ? sprintf("Inserting data for %s. Date [%s]",
                  $name, $obs_args{'date'})
        : "Inserting given files");

    unless ($obs[0]) {
        $log->debug("No observations found for instrument $name");
        return;
    }

    # Need to create a hash with keys corresponding to the observation number
    # (an array won't be very efficient since observations can be missing and
    # run numbers can be large). The values in this hash have to be a reference
    # to an array of Info::Obs objects representing each subsystem. We need to
    # construct new Obs objects based on the subsystem number.
    # $observations{$runnr}->[$subsys_number] should be an Info::Obs object.

    my %observations;
    foreach my $obs (@obs) {
        my @subhdrs = $obs->subsystems;
        $observations{$obs->runnr} = \@subhdrs;
    }

    # The %columns hash will contain a key for each table, each key's value
    # being an anonymous hash containing the column information.

    my $db = new OMP::DBbackend::Archive();
    my $dbh = $db->handle_checked();

    my %columns = map {$_ => $self->get_columns($_, $dbh)}
        qw/COMMON FILES/, $self->instrument_table();

    foreach my $runnr (sort {$a <=> $b} keys %observations) {
        my @sub_obs =  grep {$_} @{$observations{$runnr}};

        $self->insert_obs_set(
            db => $db,
            columns => \%columns,
            run_obs => \@sub_obs,
            dry_run => $dry_run,
            skip_state => $skip_state,
            %update_args);
    }
}

# For each observation:
# 1. Insert a row in the COMMON table.
# 2. Insert a row in the [INSTRUMENT] table for each subsystem used.
# 3. Insert a row in the FILES table for each subscan
#
# fails, the entire observation fails to go in to the DB.
sub insert_obs_set {
    my ($self, %arg) = @_;

    my $log = Log::Log4perl->get_logger('');

    my ($db, $run_obs, $columns, $dry_run, $skip_state) =
       map {$arg{$_}} qw/db run_obs columns dry_run skip_state/;

    my $dbh  = $db->handle();
    my @file = map {$_->simple_filename} @$run_obs;

    my %pass_arg = map {$_ => $arg{$_}} qw/columns/;
    my %common_arg = map {$_ => $arg{$_}} qw/update_only_inbeam update_only_obstime/;

    foreach (@file) {
        if (exists $self->{'_cache_touched'}->{$_}) {
            $log->debug("already processed: $_");

            return;
        }
    }

    $self->{'_cache_touched'}->{$_} = 1 foreach @file;

    for my $obs (@$run_obs) {
        my $headers = $obs->hdrhash();

        $headers = $self->munge_header_INBEAM($headers);

        if ($self->can('fill_max_subscan')) {
            $self->fill_max_subscan($headers, $obs);
        }

        if ($self->can('transform_header')) {
            my ($hash , $array) = $self->transform_header($headers);
            $obs->hdrhash($hash);
        }
    }

    my $common_obs = $run_obs->[0]
        or do {
            $log->debug('XXX First run obs is undefined|false; nothing to do.');
            return;
        };

    # Break hash tie by copying & have an explicit anonymous hash ( "\%{ ... }"
    # does not untie).  This is so that a single element array reference when
    # assigned to one of the keys is assigned as reference (not as the element
    # contained with in).
    my $common_hdrs = {%{$common_obs->hdrhash}};

    $log->debug(sprintf "[%s]...", join ', ', @file);

    if (! $arg{'process_simulation'} && $self->is_simulation($common_hdrs)) {
        $log->debug("simulation data; skipping" );

        $self->_get_xfer_unconnected_dbh()->put_state(
                state => 'simulation', files => \@file)
            unless ($dry_run or $skip_state);

        return;
    }

    if ($self->_do_verification()) {
        my $verify = JCMT::DataVerify->new(Obs => $common_obs);

        unless (defined $verify) {
            $log->logdie(join "\n",
                'Could not make JCMT::DataVerify object:',
                $common_obs->summary('text'),
                Dumper([sort $common_obs->filename()]),
            );
        }

        my %invalid = $verify->verify_headers();

        foreach (keys %invalid) {
            my $val = $invalid{$_}->[0];

            if ($val =~ /does not match/i) {
                $log->debug("$_ : $val");
                undef $common_hdrs->{$_};
            }
            elsif ($val =~ /should not/i) {
                $log->debug("$_ : $val");
                undef $common_hdrs->{$_} if $common_hdrs->{$_} =~ /^UNDEF/ ;
            }
        }
    }

    try {
        if ($arg{'calc_radec'}
                && ! $self->skip_calc_radec('headers' => $common_hdrs)) {

            unless ($self->calc_radec($common_obs, $common_hdrs)) {
                $log->debug("problem while finding bounds; skipping");

                throw JSA::Error('could not find bounds');
            }
        }

        # COMMON table.
        $db->begin_trans() if not $dry_run;

        $self->fill_headers_COMMON($common_hdrs, $common_obs);

        my $table = 'COMMON';

        $self->_update_or_insert(
            update_args => \%common_arg,
            dbhandle   => $dbh,
            table      => 'COMMON',
            table_columns => $columns->{$table},
            headers    => $common_hdrs,
            dry_run    => $dry_run);

        if ($dbh->err()) {
            my $text = $dbh->errstr();

            $db->rollback_trans();

            throw JSA::Error($text);
        }

        # FILES, ACSIS, SCUBA2 tables.
        unless ($arg{'update_only_obstime'} || $arg{'update_only_inbeam'}) {
            $self->add_subsys_obs(
                %pass_arg,
                db  => $db,
                obs => $run_obs,
                dry_run => $dry_run,
                skip_state => $skip_state);
        }

        $db->commit_trans() if not $dry_run;

        $log->debug("successful");
    }
    catch Error::Simple with {
        my $e = shift;
        my $text = $e->text();
        $log->debug('error inserting obs set: ' . $text) if $text;
        $text = 'unknown error' unless $text;

        $self->_get_xfer_unconnected_dbh()->put_state(
                state => 'error', files => \@file,
                comment => $text)
            unless ($dry_run or $skip_state);
    };
}

=item B<_do_verification>

Should we use JCMT::DataVerify?

=cut

sub _do_verification {
    my $self = shift;
    return 1;
}

sub _filter_header {
    my ($self, $obs, %ignore) = @_;

    my $log = Log::Log4perl->get_logger('');

    return unless scalar @{$obs};

    my $remove_ok = sub {
        my ($href, $key) = @_;

        return unless exists $href->{$key}
                   && defined $ignore{$key};

        my $present = $href->{$key};

        return
            defined $present
            && any {
                    looks_like_number($_)
                        ? $present == $_
                        : $present eq $_
            } (ref $ignore{$key}
                    ? @{$ignore{$key}}
                    : $ignore{$key});

        };

    my @new;

    OBS: foreach my $cur (@{$obs}) {
        my $header = $cur->hdrhash;

        IGNORE: foreach my $key (keys %ignore) {
            if ($remove_ok->($header, $key)) {
                $log->debug(sprintf
                    'Ignoring observation with %s = %s',
                    $key, $header->{$key});

                next OBS;
            }

            push @new, $cur;
            my @subhead = $header->{'SUBHEADERS'} ? @{$header->{'SUBHEADERS'}} : ();

            next OBS unless scalar @subhead;

            my @new_sub;

            SUBHEAD: foreach my $sub (@subhead) {
                if ($remove_ok->($sub, $key)) {
                    $log->debug(sprintf
                        'Ignoring subheader with %s = %s',
                        $key, $sub->{$key});

                    next SUBHEAD;
                }

                push @new_sub, $sub;
            }

            $new[-1]->{'SUBHEADERS'} = [@new_sub];
        }
    }

    return @new;
}

=item B<_get_obs_group>

When no files are provided, returns a L<OMP::Info::ObsGroup> object
given date as a hash.

    $obs = $enter->_get_obs_group(date => '20090609',
                                  dry_run => $dry_run,
                                  skip_state => $skip_state,
                                 );

Else, returns a L<OMP::Info::ObsGroup> object created with
given files.

    $obs = $enter->_get_obs_group;

Note: writes the file state in the transfer table unless the
dry_run argument is given.

=cut

sub _get_obs_group {
    my ($self, %args) = @_;
    my $dry_run = $args{'dry_run'};
    my $skip_state = $args{'skip_state'};

    my $log = Log::Log4perl->get_logger('');

    my $xfer = $self->_get_xfer_unconnected_dbh();

    my @file;

    unless (exists $args{'files'}) {
        throw JSA::Error::FatalError('Neither files nor date given to _get_obs_group')
            unless exists $args{'date'};

        # OMP uses Time::Piece (instead of DateTime).
        my $date = Time::Piece->strptime($args{'date'}, '%Y%m%d');

        @file = OMP::FileUtils->files_on_disk(
            date => $date,
            instrument => $self->instrument_name());
    }
    else {
        @file = @{$args{'files'}};
    }

    # Flatten 2-D array reference.
    @file = map {! defined $_ ? () : ref $_ ? @{$_} : $_} @file;

    return unless scalar @file;

    my @obs;
    foreach my $file (@file) {
        my $base = _basename($file);

        unless (-r $file && -s _) {
            my $ignored = 'Unreadable or empty file';

            $xfer->put_state(
                    state => 'ignored', files => [$base], comment => $ignored)
                unless $dry_run || $skip_state;

            $log->warn("$ignored: $file; skipped.\n");

            next;
        }

        $xfer->add_found([$base], '')
            unless $dry_run || $skip_state;

        my $text = '';
        my $err;

        try {
            push @obs, OMP::Info::Obs->readfile(
                $file,
                nocomments => 1,
                retainhdr  => 1,
                ignorebad  => 1,
                header_search => 'files');
        }
        catch OMP::Error::ObsRead with {
            ($err) = @_;

            #throw $err
            #  unless $err->text() =~ m/^Error reading FITS header from file/;
            $text = 'Error during file reading when making Obs:';
        }
        otherwise {
            ($err) = @_;

            $text = 'Unknown Error';
        };

        if ($err) {
            $text .=  ': ' . $err->text();

            $xfer->put_state(
                    state => 'error', files => [$base], comment => $text)
                unless $dry_run || $skip_state;

            $log->error($text);
        }
    }

    return unless scalar @obs;

    my @headers;
    for my $ob (@obs) {
        my $header = $ob->hdrhash;

        # These headers will be passed to OMP::FileUtils->merge_dupes which
        # in turn passes them to Astro::FITS::Header->new(Hash => ...).
        # That constructor drops any null or empty string headers.  Since
        # we need to see the INBEAM header for all files, replace blank
        # values with a dummy placeholder first.  (See also
        # munge_header_INBEAM where these placeholders are removed.)
        if (exists $header->{'INBEAM'}) {
            unless ((defined $header->{'INBEAM'})
                    and ($header->{'INBEAM'} ne '')) {
                $header->{'INBEAM'} = 'NOTHING';
            }
        }

        push @headers, {
            filename => $ob->{'FILENAME'}->[0],
            header => $header,
        };
    }

    my $merged = OMP::FileUtils->merge_dupes(@headers);

    @obs = OMP::Info::Obs->hdrs_to_obs(retainhdr => 1,
                                       fits      => $merged);

    return OMP::Info::ObsGroup->new(obs => \@obs);
}


=item B<skip_obs>

Returns a truth value indicating if an observation is a simulation run,
or for which RA/Dec cannot be calculated. It accepts an
L<OMP::Info::Obs> object.  If optional header hash reference (see
L<OMP::Info::Obs/hdrhash>) is not given, it will be retrieved from the
given L<OMP::Info::Obs> object.

    $skip = $enter->skip_obs($obs);

    $skip = $enter->skip_obs($obs, $header);

C<JSA::Error> exception is thrown if header hash (reference) is
undefined.

=cut

sub skip_obs {
    my ($self, $obs, $header) = @_;

    $header = $obs->hdrhash unless defined $header;

    # Alternatively could (silently) return false.
    throw JSA::Error "FITS headers are undefined."
        unless defined $header;

    # Tests are the same which control database changes.
    return $self->is_simulation($header)
        || ! $self->calc_radec($obs, $header);
}

=item B<is_simulation>

Returns a truth value to indicate if the given headers are of
"simulation".

An observation is marked as simulation if ...

    SIMULATE header has value of "T", or
    OBS_TYPE header has value of "RAMP".

    for my $obs (...) {

        ...

        next if $enter->is_simulation($obs->hdrhash());

        ...
    }

=cut

sub is_simulation {
    my ($self, $header) = @_;

    # Value changed from 'T' to 1 without notice. Now deal with both.
    my %sim = (
        'SIMULATE' => qr/^(?:[t1]|1\.0+)$/i,
        'OBS_TYPE' => qr/^ramp$/i,
    );

    # "SIMULATE" is more likely to be in the main header.
    my @order = ('SIMULATE', 'OBS_TYPE');

    foreach my $name (@order) {
      my $val = $self->_find_header(headers => $header,
                                    name   => $name,
                                    test   => 'defined',
                                    value  => 1);

      my $test = $sim{$name};

      return 1
          if defined $val
          && $val =~ $test;
    }

    return;
}


=item B<add_subsys_obs>

Adds subsystem observations, given a hash of database handle; hash
reference of observations (run number as keys, array reference of sub
headers as values); a hash reference of columns (see I<get_columns>).

The observations hash reference is for a given run number, not the
the I<OMP::Info::Objects> in its entirety.

    $ok = $enter->add_subsys_obs(dbhandle  => $dbh,
                                 columns   => \%cols,
                                 obs       => \%obs_per_runnr,
                                 dry_run   => $dry_run,
                                 skip_state=> $skip_state);

It is called by I<insert_observations> method.

=cut

sub add_subsys_obs {
    my ($self, %args) = @_;

    my $log = Log::Log4perl->get_logger('');

    foreach my $k (qw/db columns obs/) {
        next if exists $args{$k} && $args{$k} && ref $args{$k};

        throw JSA::Error::BadArgs("No suitable value given for ${k}.");
    }

    my ($db, $obs, $columns, $dry_run, $skip_state) =
        map {$args{$_}} qw/db obs columns dry_run skip_state/;

    my $dbh = $db->handle();

    my $subsysnr = 0;
    my $totsub = scalar @{$obs};

    foreach my $subsys_obs (@{$obs}) {
        $subsysnr++;
        $log->debug("Processing subsysnr $subsysnr of $totsub");

        # Obtain instrument table values from this Obs object.  Break hash tie.
        my $subsys_hdrs = {%{$subsys_obs->hdrhash}};

        # Need to calculate the frequency information
        $self->calc_freq($subsys_obs, $subsys_hdrs);

        my $grouped;
        if ($self->can('transform_header')) {
            (undef, $grouped) = $self->transform_header($subsys_hdrs);
        }

        my $added_files;
        foreach my $subh ($grouped ? @{$grouped} : $subsys_hdrs) {
            $self->_fill_headers_obsid_subsys($subh, $subsys_obs->obsid);

            my $error;

            unless ($added_files) {
                $added_files++;

                my $table = 'FILES';

                $self->_change_FILES(obs            => $subsys_obs,
                                     headers        => $subsys_hdrs,
                                     db             => $db,
                                     table          => $table,
                                     table_columns  => $columns->{$table},
                                     dry_run        => $dry_run,
                                     skip_state     => $skip_state);
            }

            if ($self->can('merge_by_obsidss')
                    && exists $subsys_hdrs->{'SUBHEADERS'}) {

                my $sys_sub = $subsys_hdrs->{'SUBHEADERS'};
                my @temp = $self->merge_by_obsidss($sys_sub);

                @{$sys_sub} = @{$temp[0]}
                    if scalar @temp;
            }

            my $table = $self->instrument_table();

            $self->_update_or_insert(
                dbhandle  => $dbh,
                table     => $table,
                table_columns => $columns->{$table},
                headers   => $subh,
                dry_run   => $dry_run);

            if ($dbh->err()) {
                my $text = $dbh->errstr();

                $db->rollback_trans() if not $dry_run;
                $log->debug("$text");

                throw JSA::Error($text);
            }
        }
    }
}

sub _handle_multiple_changes {
    my ($self, $vals) = @_;

    my $log = Log::Log4perl->get_logger('');

    # Go through the hash and work out whether we have multiple inserts
    my @have_ref;
    my $nrows = undef;
    foreach my $key (keys %$vals) {
        my $ref = ref $vals->{$key}
            or next;

        $log->logdie("Unsupported reference type in insert hash!\n")
            unless $ref eq 'ARRAY';

        my $row_count = scalar @{$vals->{$key}};
        if (defined $nrows) {
            # count rows
            $log->logdie("Uneven row count in insert hash ARRAY ref for key '$key'",
                         " ($row_count != $nrows) compared to first key '$have_ref[0]'\n")
            unless $row_count == $nrows;
        }
        else {
            $nrows = $row_count;
        }

        push @have_ref, $key;
    }

    # Now create an array of insert hashes with array references unrolled
    my @change;
    if (! @have_ref) {
        @change = ($vals);
    }
    else {
        # take local copy of the array content so that we do not damage caller hash
        my %local = map {$_ => [@{$vals->{$_}}]} @have_ref;

        # loop over the known number of rows
        foreach my $i (0 .. ($nrows-1)) {
            my %row = %$vals;

            foreach my $refkey (@have_ref) {
                $row{$refkey} = shift @{$local{$refkey}};
            }

            push @change, \%row;
        }
    }

    return \@change;
}

=item B<insert_hash>

Given a table name, a DBI database handle and a hash reference, insert
the hash contents into the table.  Basically a named insert.  Returns
the executed statement output.  (Copied from example in L<DBI>.)

In case of error, returns the value as returned by C<DBI->execute>.

    $status = $enter->insert_hash(table      => $table,
                                  dbhandle   => $dbh,
                                  insert     => \%to_insert
                                  dry_run    => $dry_run);

=cut

sub insert_hash {
    my ($self, %args) = @_;

    my ($table, $dbh, $insert, $dry_run, $conditional) =
        @args{qw/table dbhandle insert dry_run conditional/};

    my $log = Log::Log4perl->get_logger('');

    # Get the fields in sorted order (so that we can match with values)
    # and create a template SQL statement. This can be done with the
    # first hash from @insert_hashes

    my @insert_hashes = @{$insert};

    my (@prim_key);
    do {
        my $prim_key = _get_primary_key($table);
        @prim_key = ref $prim_key ? @{$prim_key} : $prim_key;
    };
    $log->logdie('Primary key not defined for table: ', $table)
        unless scalar @prim_key;

    my ($sql, $sth);

    # Conditional insert mode: for now pre-filter the list of hashes to
    # be inserted.  Could probably replace this for MySQL with a
    # INSERT ... ON DUPLICATE KEY statement at some point.
    if ($conditional) {
        my @insert_hashes_filtered = ();

        $sql = sprintf
            'SELECT COUNT(*) FROM %s WHERE %s ',
            $table,
            join(' AND ', map {" $_ = ? "} @prim_key);

        unless ($dry_run) {
            $sth = $dbh->prepare($sql)
                or $log->logdie(
                    "Could not prepare SQL statement for insert check\n",
                    $dbh->errstr);
        }

        for my $row (@insert_hashes) {
            my @prim_val = map {$row->{$_}} @prim_key;

            $log->trace('-----> SQL: ' . $sql);
            $log->trace(Dumper(\@prim_val));

            next if $dry_run;

            $sth->execute(@prim_val)
                or $log->logdie('SQL query for insert check failed: ', $dbh->errstr);

            my $result = $sth->fetchall_arrayref();

            $log->logdie('SQL query for insert check did not return 1 row')
                unless 1 == scalar @$result;

            # If there was no match (i.e. COUNT(*) = 0) then include this in
            # the filtered list of hashes.
            push @insert_hashes_filtered, $row unless $result->[0][0];
        }

        return undef unless scalar @insert_hashes_filtered;

        @insert_hashes = @insert_hashes_filtered;
    }

    my @fields = sort keys %{$insert_hashes[0]}; # sort required

    $sql = sprintf
        "INSERT INTO %s (%s) VALUES (%s)",
        $table,
        join(', ', @fields),
        join(', ', ('?') x scalar @fields);

    unless ($dry_run) {
        $sth = $dbh->prepare($sql)
            or $log->logdie(
                "Could not prepare SQL statement for insert\n",
                $dbh->errstr);
    }

    my ($sum, @file);
    # and insert all the rows
    for my $row (@insert_hashes) {
        my @values = @{$row}{@fields}; # hash slice

        $log->trace('-----> SQL: ' . $sql);
        $log->trace(Dumper(\@values));

        next if $dry_run;

        my $affected = $sth->execute(@values);

        if ($table eq 'FILES' && defined $affected && $affected > 0) {
            push @file, $row->{'file_id'};
        }

       return ($sum, scalar @file ? [@file] : ())
           unless $affected;

        $sum += $affected;
    }

    return ($sum, scalar @file ? [@file] : ());
}

=item B<prepare_update_hash>

Compare the given field values with those already in the given
database table.

Returns two lists of hashes, one corresponding to update operations
and one corresponding to insert operations which should be performed:

    Update operations:
        differ
        unique_key
        unique_val

    Insert operations:
        insert

Additional arguments may be given in hash form:

=over 4

=item update_only_inbeam

Update only the C<INBEAM> header value.

=item update_only_obstime

Update only the times for an observation.

=item update_only_obsradec

Only update obsra, obsdec and their associated tl, tr, bl, br values.

=back

=cut

sub prepare_update_hash {
    my ($self, $table, $dbh, $field_values, %args) = @_;

    my $log = Log::Log4perl->get_logger('');

    $log->logdie('prepare_update_hash cannot be used for table FILES')
        if $table eq 'FILES';

    # work out which key uniquely identifies the row
    my $unique_key = _get_primary_key($table);

    unless ($unique_key) {
        $log->logdie("No unique keys found for table name: '$table'\n");
    }

    my @unique_key = ref $unique_key ? @{$unique_key} : $unique_key ;

    my $rows = $self->_handle_multiple_changes($field_values);

    my $sql = 'select * ';

    my (%start, %end);

    if ($table eq 'COMMON') {
        my %range = JSA::DB::TableCOMMON::range_columns();
        @start{keys %range} = ();
        @end{values %range} = ();

        $sql = 'select ' . join ', ', JSA::DB::TableCOMMON::column_names();
    }

    $sql .= " from $table where "
          . join ' AND ', map {" $_ = ? "} @unique_key;

    my @update_hash;
    my @insert_hash;
    foreach my $row (@{$rows}) {
        my @unique_val = map $row->{$_}, @unique_key;

        $log->trace(Dumper(\@unique_key));
        $log->trace(Dumper(\@unique_val));

        my $ref = $dbh->selectall_arrayref($sql, {Columns=>{}}, @unique_val)
            or $log->logdie("Error retrieving existing content using [$sql]: ", $dbh->errstr, "\n");

        $log->logdie("Only retrieved partial dataset: ", $dbh->errstr, "\n")
            if $dbh->err;

        # how many rows
        my $count = scalar @{$ref};

        if (0 == $count) {
            # Row does not already exist: add to the insert list.
            $log->debug("new data to insert: " . (join ' ', @unique_val));

            push @insert_hash, $row;

            next;
        }

        $log->logdie("Should not be possible to have more than one row. Got $count\n")
            if $count > 1;

        my $indb = $ref->[0];

        my %differ = ();
        my $ymd_start = qr/^\d{4}-\d{2}-\d{2}/;
        my $am_pm_end = qr/\d\d[APM]$/;

        my $obs_date_re = qr/\bDATE.(?:OBS|END)\b/i;

        my $inbeam_re = qr/\b INBEAM \b/xi;

        # Allowed to be set undef if key from $field_values is missing, say as a
        # result of external header munging.
        my $miss_ok = _or_regex(qw/INBEAM/,
                                _suffix_start_end_headers(qw/SEEING SEEDAT/));

        my $tau_val = qr/\b(?:WVMTAU|TAU225)(?:ST|EN)\b/i;

        my $only_obstime = $table eq 'COMMON' && $args{'update_only_obstime'};

        my $only_inbeam = $table eq 'COMMON' && $args{'update_only_inbeam'};

        my $only_obsradec = $args{'update_only_obsradec'};

        foreach my $key (sort keys %{$indb}) {
            if (($only_inbeam && $key !~ $inbeam_re)
                    or ($only_obstime && $key !~ $obs_date_re)
                    or ($only_obsradec && $key !~ /^obs(?:ra|dec)/i)) {
                $log->debug("skipping field: $key (due to field restriction)");
                next;
            }

            $log->debug("testing field: $key");

            next if ($key !~ $miss_ok && ! exists $field_values->{$key});

            my $new = $field_values->{$key};
            my $old = $indb->{$key};

            next unless (defined $old || defined $new);

            $log->debug("continuing with $key");

            my %test = (
                start => exists $start{$key},
                end   => exists $end{$key},
                old   => $old,
                new   => $new,
            );

            my $in_range = any {$test{$_}} (qw/start end/);

            # INBEAM header: special handling.
            if ($key =~ $inbeam_re) {
                my $combined = $self->_combine_inbeam_values($old, $new);
                $differ{$key} = $combined;
                $log->debug($key . ' = ' . ($combined // '<undef>'));
                next;
            }

            # Not defined currently - inserting new value.
            if (defined $new && ! defined $old) {
                $differ{$key} = $new;
                $log->debug( qq[$key = ] . $new );
                next;
            }

            # Defined in DB but undef in new version - not expecting this but assume
            # this means a null.
            if (! defined $new && defined $old) {
                $differ{$key} = undef;
                $log->debug("$key = <undef>");
                next;
            }

            # Dates.
            if ($new =~ $ymd_start
                    && ($old =~ $ymd_start || $old =~ $am_pm_end)) {
                if ($in_range) {
                    $new = _find_extreme_value(%test,
                                               'new>old' => _compare_dates($new, $old));
                    $log->debug("  possible new value for $key = " . $new );
                }

                if ($new ne $old) {
                    $differ{$key} = $new;
                    $log->debug("$key = " . $new);
                }

                next;
            }

            if (looks_like_number($new)) {
                # Override range check for tau values as there is no relation between start
                # & end values; these are weather dependent.
                if ($key =~ $tau_val && $new != $old) {
                    $differ{$key} = $new;
                    $log->debug("$key = " . $new);
                }
                elsif ($in_range) {
                    $new = _find_extreme_value(%test, 'new>old' => $new > $old);

                    if ($new != $old) {
                        $differ{$key} = $new if $new != $old;
                        $log->debug("$key = " . $new);
                    }
                }
                else {
                    if ($new =~ /\./) {
                      # floating point
                      my $diff = abs($old - $new);
                      if ($diff > 0.000001) {
                          $differ{$key} = $new;
                          $log->debug("$key = " . $new);
                      }
                    }
                    elsif ( $new != $old ) {
                        $differ{$key} = $new;
                        $log->debug("$key = " . $new );
                    }
                }

                next;
            }

            # String.
            if ($new ne $old) {
                $differ{$key} = $new;
                $log->debug("$key = " . $new);
            }
        }

        $log->debug("differences to update: " . (join ' ', keys %differ));

        push @update_hash, {
            differ        => \%differ,
            unique_val    => [@unique_val],
            unique_key    => [@unique_key],
        };

    }

    return (\@update_hash, \@insert_hash);
}

sub _suffix_start_end_headers {
    return map {; "${_}ST" , "${_}EN"} @_;
}

sub _or_regex_string {
    return join '|',
        map {quotemeta($_)}
        sort {length $b <=> length $a}
        @_;
}

sub _or_regex {
    my $re = _or_regex_string(@_);
    return qr/\b(?:$re)/i;
}

sub _or_regex_suffix_start_end_headers {
    my $re = _or_regex_string( @_ );
    return qr/\b (?: $re )(?: ST|EN )/ix;
}

=item B<_get_primary_key>

Returns the primary key for a given table in C<jcmt> database.

    $primary = _get_primary_key('ACSIS');

=cut

sub _get_primary_key {
    my ($table) = @_;

    my %keys = (
        ACSIS     => 'obsid_subsysnr',
        COMMON    => 'obsid',
        FILES     => [qw/obsid_subsysnr file_id/],
        SCUBA2    => 'obsid_subsysnr',
        transfer  => 'file_id',
    );

    return unless exists $keys{$table};
    return $keys{$table};
}

=item B<update_hash>

Given a table name, a DBI database handle and a hash reference,
retrieve the current data values based on OBSID or OBSID_SUBSYSNR,
decide what has changed and update the values.

    $enter->update_hash($table, $dbh, \%to_update, dry_run => $dry_run);

No-op for files table at the present time.

=cut

sub update_hash {
    my ($self, $table, $dbh, $change, %args) = @_;

    return if $table eq 'FILES'
           || ! $change;

    my $dry_run = $args{'dry_run'};

    my $log = Log::Log4perl->get_logger('');

    my @change = @{$change};
    my @sorted = sort keys %{$change[0]->{'differ'}};

    return 1 unless scalar @sorted;

    my @unique_key = @{$change[0]->{'unique_key'}};

    # Now have to do an UPDATE
    my $changes = join ', ', map {" $_ = ? "} @sorted;

    my $sql = sprintf "UPDATE %s SET %s WHERE %s",
                      $table,
                      $changes,
                      join ' AND ', map {" $_ = ? "} @unique_key;

    $log->trace($sql);

    unless ($dry_run) {
        my $sth = $dbh->prepare($sql)
            or $log->logdie("Could not prepare sql statement for UPDATE\n", $dbh->errstr, "\n");

        foreach my $row (@change) {
            my @bind = map {$row->{'differ'}{$_}} @sorted;
            push @bind, @{$row->{'unique_val'}};

            my $status = $sth->execute(@bind);
            throw JSA::Error::DBError 'UPDATE error: ' . $dbh->errstr() . "\n... with { $sql, @bind }"
                if $dbh->err();

            return $status;
        }
    }

    return 1;
}

=item B<transform_value>

Given a hash of columns, and value to be inserted in a table,
alter the value if the database expects the value to be in a different
format than that of the headers.

    $enter->transform_value($table_columns, \%values);

=cut

sub transform_value {
    my ($self, $table_columns, $values) = @_;

    my $log = Log::Log4perl->get_logger('');

    # Transform boolean data hash.  Contains a hash mapping
    # values from the headers to the values the database expects.
    my %transform_bool = (
        T => 1,
        F => 0,
    );

    foreach my $column (keys %$values) {
        # Store column's current value
        my $val = $values->{$column};
        next unless defined($val);

        if (exists $table_columns->{$column}) {
            # Column is defined for this table, get the data type
            my $data_type = $table_columns->{$column};

            if ($data_type eq 'datetime') {
                # Temporarily (needs to be handled at the header source) set a
                # zero date (0000-00-00T00:00:00) to undef.
                (my $non_zero = $val) =~ tr/0T :-//d;

                unless ($non_zero) {
                    undef $values->{$column};

                    $log->trace(sprintf
                        "Converted date [%s] to [undef] for column [%s]",
                        $val, $column);
                }
            }
            elsif ($data_type =~ /^tinyint/ or $data_type =~ /^int/) {
                if (exists $transform_bool{$val}) {
                    # This value needs to be transformed to the new value
                    # defined in the %transform_bool hash
                    $values->{$column} = $transform_bool{$val};

                    $log->trace(sprintf
                        "Transformed value [%s] to [%s] for column [%s]",
                        $val, $values->{$column}, $column);
                }
            }
            elsif ($column eq 'lststart' or $column eq 'lstend') {
                # Convert LSTSTART and LSTEND to decimal hours
                my $ha = new Astro::Coords::Angle::Hour($val, units => 'sex');
                $values->{$column} = $ha->hours;

                $log->trace(sprintf
                    "Converted time [%s] to [%s] for column [%s]",
                    $val, $values->{$column}, $column);
            }
        }
    }

    return 1;
}

=item B<fill_headers_COMMON>

Fills in the headers for C<COMMON> database table, given a headers
hash reference and an L<OMP::Info::Obs> object.

    $enter->fill_headers_COMMON(\%header, $obs);

=cut

sub fill_headers_COMMON {
    my ($self, $header, $obs) = @_;

    my $log = Log::Log4perl->get_logger('');

    my $release_date = calculate_release_date($obs);

    $header->{'release_date'} = $release_date->strftime('%F %T');

    $log->trace(sprintf
        "Created header [release_date] with value [%s]",
        $header->{'release_date'});

    if (exists $header->{'INSTRUME'} && ! defined $header->{'BACKEND'}) {
        $header->{'BACKEND'} = $header->{'INSTRUME'};
    }

    _fix_dates($header);
    return;
}

# Sybase ASE 15 cannot convert '0.000000000000000e+00' to a datetime value.  Set
# those to undef, thus NULL.
sub _fix_dates {
    my ($header) = @_;

    my $date_re = qr/ (?: \b date | dat(?: en | st )\b ) /xi;

    my $zero_date_re = qr/^0{4} -? 00 -? 00/x;

    foreach my $k (keys %{$header}) {
        next unless $k =~ $date_re;

        my $date = $header->{$k};

        undef $header->{ $k }
            if ! $date
            || $date =~ $zero_date_re
            || (looks_like_number($date) && 0 == $date);
    }

    return;
}

=item B<fill_headers_FILES>

Fills in the headers for C<FILES> database table, given a
headers hash reference and an L<OMP::Info::Obs> object.

    $enter->fill_headers_FILES(\%header, $obs);

=cut

sub fill_headers_FILES {
    my ($self, $header, $obs) = @_;

    my $log = Log::Log4perl->get_logger('');

    # Create file_id - also need to extract NSUBSCAN from subheader if we have more
    # than one file. (although simply using a 1-based index would be sufficient)
    my @files = $obs->simple_filename;
    $header->{'file_id'} = \@files;

    # We need to know whether a nsubscan header is even required so %columns really
    # needs to be accessed. For now we kluge it.
    unless (exists $header->{'nsubscan'}) {
        if (scalar(@files) > 1) {
            $header->{'nsubscan'} =
                [map {$_->value('NSUBSCAN')} $obs->fits->subhdrs];
        }
        elsif (exists $header->{'NSUBSCAN'}) {
            # not really needed because the key becomes case insensitive
            $header->{'nsubscan'} = $header->{'NSUBSCAN'};
        }
        else {
            $log->logdie("Internal error - NSUBSCAN does not exist yet there is only one file!\n");
        }
    }

    $log->trace(sprintf
        "Created header [file_id] with value [%s]",
        join ',', @{$header->{'file_id'}});

    $self->_fill_headers_obsid_subsys($header, $obs->obsid);

    return;
}


=item B<munge_header_INBEAM>

Given a header hash reference, removes all the I<INBEAM> header occurrences
which have C<SHUTTER>; combines any remaining header values (in subheaders) in a
space separated list. Returns a possibly changed header hash reference.

    $changed = $enter->munge_header_INBEAM($header_hash);

=cut

sub munge_header_INBEAM {
    my ($self , $headers) = @_;

    my $name = 'INBEAM';

    # Find INBEAM values, but remove dummy placeholder values.
    # (See also _get_obs_group where these are inserted.)
    my @val = map {($_ eq 'NOTHING') ? undef : $_} $self->_find_header(
        headers => $headers,
        name   => $name,
        value  => 1,
    );

    $headers->{$name} = (scalar @val)
        ? $self->_combine_inbeam_values(@val)
        : undef;

    return $headers;
}

=item B<_combine_inbeam_values>

Combine multiple INBEAM header values.  Should be passed a list of
header values, each of which is a space-separated list of pieces of
equipment in the beam.  Rules for combining the values are as follows:

=over 4

=item

"shutter" should be removed unless present in all entries.

This is because many observations include sequences with the
shutter closed before (and/or after) main science sequence,
and we do not want to label the whole observation as having
the shutter in the beam because of that.

=item

Other entries are kept only if they appear without shutter,
unless shutter appears everywhere.

The reason for this is just in case a piece of equipment
is left in the beam from a previous observation for initial
closed-shutter sequences.  So if we observed the sky (i.e.
the shutter was not in the beam) we are only interested in
what was in the beam at that time.

=back

These rules need to work both when combining the headers
from several data files, and when merging a new value with
an existing value in the database in an incremental update
situation.

Returns a space-separated list of names in lower case,
sorted in alphabetical order to aid testing.  Undef is
returned if there are no entries to report.

=cut

sub _combine_inbeam_values {
    my $self = shift;

    my $n = 0;
    my $n_shutter = 0;

    my %entry_all = ();
    my %entry_wo_shutter = ();

    foreach (@_) {
        $n ++;

        next unless defined $_;

        my $shutter = 0;
        my @non_shutter = ();

        foreach (split ' ', lc($_)) {
            if ($_ eq 'shutter') {
                $shutter = 1;
            }
            else {
                $entry_all{$_} = 1;
                push @non_shutter, $_;
            }
        }

        if ($shutter) {
            $n_shutter ++;
        }
        else {
            $entry_wo_shutter{$_} = 1 foreach @non_shutter;
        }
    }

    my @vals;

    if ($n and $n == $n_shutter) {
        # Everything has shutter: include shutter and all the values.

        @vals = ('shutter', keys %entry_all);
    }
    else {
        # Not everything has shutter: return only those entries which
        # appear without it.
        @vals = keys %entry_wo_shutter;
    }

    # Nothing: return undef.
    return undef unless scalar @vals;

    return join(' ', sort {$a cmp $b} @vals);
}

=item B<get_columns>

Given a table name and a DBI database handle object, return a hash reference
containing columns with their associated data types.

    $cols = $enter->get_columns($table, $dbh)

=cut

sub get_columns {
    my ($self, $table, $dbh) = @_;

    throw JSA::Error('get_columns: database handle is undefined')
        unless defined $dbh;

    # Do query to retrieve column info
    my $col_href = $dbh->selectall_hashref("SHOW COLUMNS FROM $table", "Field")
        or throw JSA::Error
            "Could not obtain column information for table [$table]: "
            . $dbh->errstr . "\n";

    my %result;
    for my $col (keys %$col_href) {
        $result{$col} = $col_href->{$col}{'Type'};
    }

    return \%result;
}

=item B<get_insert_values>

Given a hash of a table columns and a hash reference containing observation
headers, return a hash reference with the table's columns as the keys,
and the insertion values as the values.

    $vals = $enter->get_insert_values(\%table_columns, \%headers);

=cut

sub get_insert_values {
    my ($self, $table_columns, $headers) = @_;

    # Map headers to columns, translating from the dictionary as
    # necessary.
    my $main = $self->extract_column_headers($table_columns, $headers);

    # Do value transformation
    $self->transform_value($table_columns, $main);

    return $main;
}

sub extract_column_headers {
    my ($self, $table_columns, $hdrhash) = @_;

    my $log = Log::Log4perl->get_logger('');

    my $dict = $self->get_dictionary();

    my %values;

    foreach my $header (sort {lc $a cmp lc $b} keys %$hdrhash) {
        my $alt_head = lc $header;

        if (exists $table_columns->{$alt_head}) {
            $values{ $alt_head } = $hdrhash->{$header};
        }
        elsif (exists $dict->{$alt_head}
                && exists $table_columns->{$dict->{$alt_head}}) {
            # Found header alias in dictionary and column exists in table
            my $alias = $dict->{$alt_head};
            $values{$alias} = $hdrhash->{$header};

            $log->trace("  MAPPED header [$header] to column [$alias]");
        }

        $log->trace("  Could not find alias for header [$header].  Skipped.")
            unless exists $values{$alt_head};
    }

    return \%values;
}

=item B<create_dictionary>

Return a hash reference containing the dictionary contents.

    $dictionary = JSA::EnterData->create_dictionary($filename);

=cut

sub create_dictionary {
    my ($class, $dictionary) = @_;

    my %dict;

    my $log = Log::Log4perl->get_logger('');

    open my $DICT, '<', $dictionary
        or $log->logdie("Could not open data dictionary '$dictionary': $!\n");

    my @defs = grep {$_ !~ /^\s*(?:#|$)/} <$DICT>;  # Slurp!

    close $DICT
        or $log->logdie("Error closing data dictionary '$dictionary': $!\n");

    foreach my $def (@defs) {
        $def =~ s/\s+$//;

        if ( $def =~ /(.*?)\:\s(.*)/ ) {
            # Store each dictionary alias as a key whose value is a column name
            map {$dict{$_} = "$1"} split /\s/, "$2";
        }
    }

    return \%dict;
}

=item B<skip_obs_calc>

Given a hash of C<headers> key with L<OMP::Info::Obs> object header hash
reference (or C<obs> key & L<OMP::Info::Obs> object); and C<test> as key & hash
reference of header name and related values as regular expression, returns a
truth value if observation should be skipped.

Throws L<JSA::Error::BadArgs> exception when headers (or L<*::Obs> object) are
missing or C<test> hash reference value is missing.

    print "skipped obs"
        if $enter->skip_obs_calc(
            headers => $obs->hdrhash(),
            test => {
                'OBS_TYPE' => qr/\b(?: skydip | FLAT_?FIELD  )\b/xi
            });

=cut

sub skip_obs_calc {
    my ($self, %arg) = @_;

    my $log = Log::Log4perl->get_logger('');

    # Skip list.
    my %test =
        exists $arg{'test'} && defined $arg{'test'} ? %{$arg{'test'}} : ();

    scalar keys %test
        or throw JSA::Error::BadArgs('No "test" hash reference given.');

    my $header;
    if (exists $arg{'headers'}) {
        throw JSA::Error::BadArgs('No "headers" value given to check if to find bounding box.')
            unless defined $arg{'headers'};

        $header = $arg{'headers'};
    }
    else {
        JSA::Error::BadArgs('No "obs" value given to check if to find bounding box.')
            unless exists $arg{'obs'} && defined $arg{'obs'};

        JSA::Error::BadArgs("Could not get header hash from \"$arg{'obs'}\"")
            unless $header = $arg{'obs'}->hdrhash();
    }

    foreach my $name (sort keys %test) {
        $self->_find_header(headers => $header,
                            name    => $name,
                            value_regex => $test{$name})
            or next;

        $log->debug("Matched \"$name\" with $test{$name}; obs may be skipped.");

        return 1;
    }

    return;
}

=item B<skip_calc_radec>

Given a C<OMP::Info::Obs> object header hash reference -- or an
C<OMP::Info::Obs> object -- as a hash, returns a truth value if
bounding box calculation should be skipped.

    print "skipped calc_radec()"
        if $enter->skip_calc_radec(headers => $obs->hdrhash());

Default skip list is ...

    'OBS_TYPE' => qr/\b skydips? \b/ix

Optionally accepts a skip list with I<skip> as key name, and a hash
reference as value of header names as keys and header values as
regular expressions ...

    print "skipped calc_radec()"
        if $enter->skip_calc_radec(
            headers => $obs->hdrhash(),
            test => {
                'OBS_TYPE' => qr/\b(?: skydip | FLAT_?FIELD  )\b/xi
            });

=cut

sub skip_calc_radec {
    my ($self, %arg) = @_;

    my $skip = qr/\b skydips? \b/xi;

    return $self->skip_obs_calc(test => {OBS_TYPE => $skip}, %arg);
}

=item B<calc_radec>

Calculate RA/Dec extent (ICRS) of the observation and the base
position.  It populates header with corners of grid (in decimal
degrees).  Status is perl status: 1 is good, 0 bad.

    $status = JSA::EnterData->calc_radec($obs, $header);

=cut

sub calc_radec {
    my ($self, $obs, $headerref) = @_;

    my $log = Log::Log4perl->get_logger('');

    # File names for a subsystem
    my @filenames = $obs->filename;

    # Now need to write these files to a temp file
    my $temp = File::Temp->new(template => '/tmp/radec-XXXXXXXXXX');
    $temp->unlink_on_destroy(1);
    write_list($temp->filename(), [@filenames]);

    # PA (may not be present)
    my $pa = $headerref->{'MAP_PA'};
    $pa *= -1 if defined $pa;

    my @command  = $self->get_bound_check_command($temp->filename(), $pa);

    $log->info(sprintf(
        "Performing bound calculation for files starting %s", $filenames[0]));

    # Get the bounds
    my @corner     = qw/TL BR TR BL/;
    my %par_corner = map {; $_ => 'F' . $_ } @corner;

    my $values = try_star_command(
        command => \@command,
        values => [qw/REFLAT REFLON/, values %par_corner]);

    return unless defined $values;

    my %result = (REFLAT => undef,
                  REFLON => undef);

    foreach my $k (sort values %par_corner) {
        my $res = $values->{$k};

        # Rarely happens but when it does, produces warnings about operations on
        # undef values.
        # XXX Need to ask if it is important enough to log.
        unless ($res) {
            $log->logwarn("No value found for parameter $k");
            # XXX return from sub instead?
            next;
        }

        $result{$k} = [
            map {
                Astro::Coords::Angle->new($_, units => 'rad')
            } split(/\s+/,$res)];
    }

    foreach my $corner (@corner) {
        my $parkey = $par_corner{$corner};
        my $radec  = exists $result{$parkey} ? $result{$parkey} : undef;

        next unless defined $radec;

        my $alt = lc $corner;
        $headerref->{"obsra$alt"}  = $radec->[0]->degrees;
        $headerref->{"obsdec$alt"} = $radec->[1]->degrees;
    }

    # and the base position (easier to just ask SMURF rather than opening the file) but
    # for a planet or comet/asteroid this will not be correct and should be set to undef
    # This means we have to look at JCMTSTATE anyway (but we still ask SMURF because that
    # will save us doing coordinate conversion)

    my $tracksys = $self->_find_header(headers => $headerref,
                                       name   => 'TRACKSYS',
                                       value  => 1,
                                       test   => 'true');

    my %state;
    unless ($tracksys) {
        %state = read_jcmtstate($filenames[0], 'start', qw/TCS_TR_SYS/);
        $log->logdie("Error reading state information from file $filenames[0]\n")
            unless keys %state;
    }

    my $not_app_azel = sub {
        return defined $_[0]
               && length $_[0]
               && $_[0] !~ /^(?:APP|AZEL)/i
    };

    # check for APP or AZEL (should never be AZEL!)
    if ($not_app_azel->($tracksys)
            || (exists $state{'TCS_TR_SYS'}
                && $not_app_azel->($state{'TCS_TR_SYS'}))) {
        foreach my $k (qw/REFLON REFLAT/) {
            $result{$k} = $values->{$k};
        }

        # convert to radians
        $result{'REFLON'} = Astro::Coords::Angle::Hour->new(
            $result{'REFLON'}, units => 'sex', range => '2PI')->degrees
            if defined $result{'REFLON'};

        $result{'REFLAT'} = Astro::Coords::Angle->new(
            $result{'REFLAT'}, units => 'sex', range => 'PI')->degrees
            if defined $result{'REFLAT'};
    }

    $headerref->{'obsra'}  = $result{'REFLON'};
    $headerref->{'obsdec'} = $result{'REFLAT'};

    return 1;
}

sub _change_FILES {
    my ($self, %arg) = @_;

    my $log = Log::Log4perl->get_logger('');

    my ($headers, $obs, $db, $table, $table_columns, $dry_run, $skip_state) =
        @arg{qw/headers obs db table table_columns dry_run skip_state/};

    throw JSA::Error('_change_FILES: columns not defined')
        unless defined $table_columns;

    $log->trace(">Processing table: $table");

    my $dbh = $db->handle();

    # Create headers that don't exist
    $self->fill_headers_FILES($headers, $obs);

    my $insert_ref = $self->get_insert_values($table_columns, $headers);

    my ($files , $error);
    try {
        throw JSA::Error "Empty hash reference in _change_FILES."
            unless scalar keys %$insert_ref;

        _verify_file_name($insert_ref->{'file_id'});

        my $hash = $self->_handle_multiple_changes($insert_ref);

        ($error, $files) = $self->insert_hash(
            table     => $table,
            dbhandle  => $dbh,
            insert    => $hash,
            dry_run   => $dry_run,
            conditional => 1);

        $error = $dbh->errstr
            if $dbh->err();
    }
    catch JSA::Error with {
        $error = shift @_;
    };

    if ( $dbh->err() ) {
        $db->rollback_trans() if not $dry_run;

        $log->debug($error) if defined $error;

        return;
    }

    if ((not ($dry_run || $skip_state)) and $files and scalar @{$files}) {
        my $xfer = $self->_get_xfer_unconnected_dbh();
        $xfer->put_state(
            state => 'ingested', files => [map _basename($_), @{$files}]);
    }

    return;
}

=item B<_update_or_insert>

It is a wrapper around I<update_hash> and I<insert_hash> methods.
It calls C<prepare_update_hash> to identify the necessary insert and
update operations, and then calls the above methods as appropriate.

Given a hash with C<table>, C<table_columns>, C<headers> as keys.  For details
about values, see I<insert_hash>, I<update_hash>, and I<get_insert_values>
methods.

    $enter->_update_or_insert(%hash);

=cut

sub _update_or_insert {
    my ($self, %args) = @_;

    my $table = $args{'table'};
    my $table_columns = $args{'table_columns'};
    my $headers = $args{'headers'};
    my $dry_run = $args{'dry_run'};

    throw JSA::Error('_update_or_insert: columns not defined')
        unless defined $table_columns;

    my $log = Log::Log4perl->get_logger('');

    $log->trace(">Processing table: $table");

    my $vals = $self->get_insert_values($table_columns, $headers);

    my $update_args = $args{'update_args'} // {};

    my ($change_update, $change_insert) = $self->prepare_update_hash(
        @args{qw/table dbhandle/}, $vals, %$update_args);

    if (scalar @$change_insert) {
        $change_insert = $self->_apply_kludge_for_COMMON($change_insert)
            if 'COMMON' eq $table ;

        $self->insert_hash(
            insert => $change_insert,
            dry_run => $dry_run,
            map {$_ => $args{$_}} qw/table dbhandle/);
    }

    if (scalar @$change_update) {
        $self->update_hash(@args{qw/table dbhandle/}, $change_update,
                           dry_run => $dry_run);

    }
}

# KLUDGE to avoid duplicate inserts due to same obsid.  First hash reference
# most likely have undef (AZ|AM|EL)(START|END).
sub _apply_kludge_for_COMMON {
    my ($self, $vals) = @_;

    return unless ref $vals eq 'ARRAY'
               || 1 < scalar @{$vals};

    my %val;
    for my $v (@{$vals}) {
        # Last one "wins".
        $val{$v->{'obsid'}} = $v;
    }

    return [map {$val{$_}} keys %val];
}

=item B<_find_header>

Returns a list of header values or a truth value, given a hash with
I<headers> and I<name> as the required keys. Respective hash values
are a header hash reference and header name to search for.  B<Default>
behaviour is to B<return a truth value if the given header exists>.
Returns nothing if the header is missing or specified test fails.
C<SUBHEADERS> are also searched along with the main header hash.

    print 'OBSEND header exists'
        if $enter->_find_header('headers' => $hdrhash,
                                'name' => 'OBSEND');

Optional keys are ...

=over 2

=item I<test> "true" | "defined"

To Test for the header value being true or defined by providing
I<test> key with value of "true" or "defined".

    print 'OBSEND header value is defined'
        if $enter->_find_header('headers' => $hdrhash,
                                'name' => 'OBSEND',
                                'test' => 'defined');

=item I<value> any value

To receive header value when defined, specify the I<value> key (with
any value).

    use Data::Dumper;
    print "OBSEND header value if present: ",
        Dumper( $enter->_find_header('headers' => $hdrhash,
                                     'name'  => 'OBSEND',
                                     'value' => undef));

=item I<value_regex> regex

To actually match header value, specify I<value_regex> key with value
of a regular expression, in which case I<C<value> is ignored>.

    print "OBS_TYPE is 'skydip'."
        if $enter->_find_header('headers' => $hdrhash,
                                'name'  => 'OBS_TYPE',
                                'value_regex' => qr/\b skydip \b/xi);

=back

=cut

sub _find_header {
    my ($self, %args) = @_;
    my ($head, $name, $val_re) =
      @args{qw/headers name value_regex/};

    defined $val_re && ! ref $val_re
        and $val_re = qr/\b${val_re}\b/x;

    my $test = sub {
        my ($head, $key) = @_;

        return unless exists $head->{$key};
        foreach ($args{'test'}) {
            last unless defined $args{'test'};

            return !! $head->{$key} if $_ eq 'true';

            return defined $head->{ $key } if $_ eq 'defined';
        }

        return 1;
    };

    my $array = ref $head eq 'ARRAY';

    foreach my $h ($array ? @{$head} : $head) {
        my $val = $test->($h, $name) ? $h->{$name} : undef;

        if (defined $val) {
            return $val =~ $val_re if defined $val_re;

            return 1 unless exists $args{'value'};

            return $val unless wantarray;

            $args{'store'}->{$val} = undef;
        }
    }

    if (wantarray and defined $args{'store'}) {
        my %seen;
        return grep ! $seen{$_} ++,
               keys %{$args{'store'}};
    }

    # Only one level of indirection is checked, i.e. header inside "SUBHEADER"
    # pseudo header with array reference of hash references as value.
    return if $array;

    my $subh = 'SUBHEADERS';
    return $self->_find_header(%args, 'headers' => $head->{$subh})
        if exists $head->{$subh};

    return;
}

=item B<_verify_file_name>

Verifies that a file name is in format matching
C<{^ a 2\d{7} _ \d+ _ \d+ _ \d+ \. sdf $}x>, e.g.
C<a20080726_00001_01_0001.sdf>.  File names can be given either as
plain scalar or in an array reference.

Throws C<JSA::Error> with a message listing all the file names in
unexpected format.  Else, it simply returns.

=cut

sub _verify_file_name {
    my ($name) = @_;

    return unless defined $name;

    my @bad;
    for my $n (ref $name ? @{$name} : $name) {
        push @bad, $n unless looks_like_rawfile($n);
    }

    my $size = scalar @bad;

    return unless $size;

    throw JSA::Error sprintf "Bad file name%s: %s\n",
                             ($size > 1 ? 's' : ''), join ', ', @bad ;
}

=item B<_get_xfer_unconnected_dbh>

Get a JSA::DB::TableTransfer object, to be created as needed.
This method uses a new database handle
unconnected to the one used elsewhere.  (Note it's not entirely
unconnected -- if the default (or a previous) name is used, then
a cached object is returned.

=cut

{
    my %xfer;

    sub _get_xfer_unconnected_dbh {
        my ($self, $name) = @_;

        $name ||= 'xfer-new-dbh';

        return $xfer{$name} if exists $xfer{$name};

        my $db = new JSA::DB(name => $name);
        $db->use_transaction(0);

        return $xfer{$name} = new JSA::DB::TableTransfer(
            db => $db, transactions => 0);
    }
}

sub _compare_dates {
    my ($new, $old) = @_;

    # Sometimes a date-time value only has date, in which case time is appended
    # without a 'T'.
    $new =~ s/ /T/;

    $new = make_datetime($new);
    $old = make_datetime($old);

    return $new > $old;
}

sub _find_extreme_value {
    my (%arg) = @_;

    my $gt = $arg{'new>old'};
    my ($old, $new, $start, $end) = @arg{qw/old new start end/};

    # Smaller|earlier value.
    if ($start) {
        return ! $gt ? $new : $old;
    }

    # Larger|later value.
    if ($end) {
        return $gt ? $new : $old;
    }

    throw JSA::Error "Neither 'start' nor 'end' type was specified";
}

sub _basename {
    return unless scalar @_;

    require File::Basename;
    my ($base) = File::Basename::fileparse($_[0]);
    return $base;
}

# Note: methods below were imported from the calcbounds script.

sub calcbounds_find_files {
    my $self = shift;
    my %opt = @_;

    my $date = _reformat_datetime($opt{'date'});

    my $log = Log::Log4perl->get_logger('');

    my @file;
    unless ($opt{'avoid-db'}) {
        @file = $self->calcbounds_files_from_db(date => $date, obs_types => $opt{'obs_types'})
            or $log->info('Did not find any file paths in database.');
    }
    else {
        $log->error_die('No date given to find files.')
            unless $date;

        $log->info('Avoiding database for file paths.');

        @file = $self->calcbounds_files_for_date(date => $date)
            or $log->error('Could not find any readbles files for ',
                           'given file paths, file list, or date.');
    }

    return \@file;
}

sub calcbounds_files_for_date {
    my $self = shift;
    my %opt = @_;

    my $date_string = $opt{'date'};

    my $log = Log::Log4perl->get_logger('');

    $log->debug('Finding files for date ', $date_string);

    # O::FileUtils requires date to be Time::Piece object.
    my $date = Time::Piece->strptime($date_string, '%Y%m%d');

    my @file;

    $log->debug('finding files for instrument ', $self->instrument_name());

    push @file, OMP::FileUtils->files_on_disk(date       => $date,
                                              instrument => $self->instrument_name());

    $log->debug('Files found for date ', $date_string, ' : ', scalar @file);

    # Expand array references whcih come out from FileUtils sometimes.
    return map {$_ && ref $_ ? @{$_} : $_} @file;
}

sub calcbounds_files_from_db {
    my $self = shift;
    my %opt = @_;

    my $date = $opt{'date'};
    my $obs_types = $opt{'obs_types'};

    my $log = Log::Log4perl->get_logger('');

    my $pattern = lc sprintf '%s%%', substr $self->instrument_name(), 0, 1;
    $pattern = sprintf '%s%s%%', $pattern, $date if $date;

    $log->info('Getting file paths from database matching ', $pattern);

    # obsra & -dec may be null but not *{tl,tr,bl,br} if bounds do exist.
    my $sql = sprintf('SELECT f.file_id
        FROM COMMON c, FILES f
        WHERE c.obsid = f.obsid
          AND f.file_id like ?
          AND c.utdate = ?
          AND c.obs_type IN ( %s )
          AND ( c.obsra IS NULL and c.obsdec IS NULL )',
        join(',', ('?') x scalar @$obs_types));

    my $jdb = new JSA::DB();
    my $tmp = $jdb->run_select_sql(
        sql    => $sql,
        values => [$pattern, $date, @$obs_types]);

    my @file;
    @file = $self->make_raw_paths(map {$_->{'file_id'}} @{$tmp})
        if $tmp
        && ref $tmp
        && scalar @{ $tmp };

    $log->debug('Found file paths in database: ' , scalar @file);

    return @file;
}

sub calcbounds_update_bound_cols {
    my ($self, %arg) = @_;
    my $dry_run = $arg{'dry_run'};
    my $skip_state = $arg{'skip_state'};
    my $skip_state_found = $arg{'skip_state_found'};
    my $obs_types = $arg{'obs_types'};

    my %obs_args = ();
    $obs_args{'date'} = _reformat_datetime($arg{'date'}) if exists $arg{'date'};
    $obs_args{'files'} = _unique_files($arg{'files'}) if exists $arg{'files'};

    my $n_err = 0;

    my $process_obs_re = join '|', @$obs_types;
       $process_obs_re = qr{\b( $process_obs_re )}xi;

    my $obs_list = $self->calcbounds_make_obs(
            dry_run => $dry_run,
            skip_state => ($skip_state or $skip_state_found),
            %obs_args)
        or return;

    my $log = Log::Log4perl->get_logger('');

    my @bound =
        # ";" is to indicate to Perl that "{" starts a BLOCK not an EXPR.
        map {; "obsra$_" , "obsdec$_"} ('', qw/tl bl tr br/);

    my $db = new OMP::DBbackend::Archive();
    my $dbh = $db->handle_checked();

    my $table = 'COMMON';
    my %pass = (
        dbhandle => $dbh,
        table    => $table,
        table_columns  => $self->get_columns($table, $dbh),
    );

    for my $obs (@{$obs_list}) {
        my @subsys_obs = $obs->subsystems()
          or next;

        my $common = $subsys_obs[0];
        my %header = %{$common->hdrhash()};

        my $obs_type;
        foreach my $name (map {; $_ , uc $_ } qw/obstype obs_type/) {
            if ( exists $header{$name} ) {
                $obs_type = $header{$name};
                last;
            }
        }

        next unless $obs_type;

        my $found_type = ($obs_type =~ $process_obs_re)[0]
            or next;

        my @file_id = map {$_->simple_filename()} @subsys_obs;

        $log->info(join "\n    ", 'Processing files', @file_id);

        if ($self->instrument_name() eq 'SCUBA-2') {
            unless (_calcbounds_any_header_sub_val(\%header, 'SEQ_TYPE', $found_type)) {
                $log->debug('  skipped uninteresting SEQ_TYPE');
                next;
            }
        }

        if ($self->calcbounds_find_dark(\%header)) {
            $log->debug('  skipped dark.');
            next;
        }

        _fix_dates(\%header);

        $log->debug('  calculating bounds');

        unless ($self->calc_radec($common, \%header)) {
            $log->error('  ERROR  while finding bounds');

            unless ($dry_run or $skip_state) {
                $log->debug('Setting file paths with error state');
                my $xfer = $self->_get_xfer_unconnected_dbh();
                $xfer->put_state(
                        state => 'error', files => \@file_id,
                        comment => 'bound calc');
            }

            $n_err ++;
            next;
        }

        unless (any {exists $header{$_}} @bound) {
            $log->warn('  did not find any bound values.');
            return;
        }

        $log->info('  UPDATING headers with bounds');

        $self->_update_or_insert(%pass,
                                 headers    => \%header,
                                 dry_run    => $dry_run,
                                 update_args => {update_only_obsradec => 1});
    }

    return $n_err;
}

sub calcbounds_make_obs {
    my ($self, %opt) = @_;
    my $dry_run = $opt{'dry_run'};
    my $skip_state = $opt{'skip_state'};

    my %obs_args = ();
    $obs_args{'date'} = $opt{'date'} if exists $opt{'date'};
    $obs_args{'files'} = $opt{'files'} if exists $opt{'files'};

    my $log = Log::Log4perl->get_logger('');

    my $group = $self->_get_obs_group(
            dry_run => $dry_run,
            skip_state => $skip_state,
            %obs_args)
        or do {
            $log->warn('Could not make obs group.');
            return;
        };

    my @obs = $group->obs()
        or do {
            $log->warn( 'Could not find any observations.' );
            return;
        };

    # Note: this was previously SCUBA-2 specific, but can probably
    # be called harmlessly for ACSIS since it doesn't have flatfields.
    @obs = $self->_filter_header(\@obs, 'OBS_TYPE' => [qw/FLATFIELD/]);

    return unless scalar @obs;
    return \@obs;
}

sub calcbounds_find_dark {
    my ($self, $header) = @_;

    return unless $self->can('_is_dark');

    my $dark = $self->_is_dark($header);
    foreach my $sh (exists $header->{'SUBHEADERS'}
                         ? @{$header->{'SUBHEADERS'}}
                         : ()) {
        $dark =  $self->_is_dark($sh)
          or last;
    }

    return $dark;
}

=back

=head2 FUNCTIONS

=over 2

=item B<calculate_release_date>

Calculate the release date to be written into the COMMON table
given an OMP::Info::Obs object.

    my $release_date = calculate_release_date($obs);

Create release date (end of semester + one year) for the general
case but for OBSTYPE != SCIENCE or STANDARD=T the release date is
immediate.

CLS special since its data is protected until one year after the
end of semester 14B.

=cut

sub calculate_release_date {
    my $obs = shift;

    # Get date of observation
    my $obsdate = $obs->utdate;

    if ( $obs->projectid =~ /^mjlsc/i && $obs->isScience) {
        # CLS. Should properly check the SURVEY FITS header
        return DateTime->new(month => 3,
                             year => 2016,
                             day => 1,
                             hour => 23,
                             minute => 59,
                             second => 59,
                             time_zone => 'UTC');

    }
    elsif ($obs->projectid =~ /ec05$/i && $obs->isScience) {
        # EC05 is a public calibrator monitoring project
        return DateTime::Format::ISO8601->parse_datetime($obsdate);

    }
    elsif ($obs->projectid =~ /ec/i) {
        # Do not release EC data.

        return DateTime->new(month => 1,
                             year => 2031,
                             day => 1,
                             hour => 0,
                             minute => 0,
                             second => 0,
                             time_zone => 0);

    }
    elsif ($obs->isScience) {
        # semester release
        my $semester = OMP::DateTools->determine_semester(date => $obsdate,
                                                          tel => 'JCMT');
        my ($sem_begin, $sem_end) =
            OMP::DateTools->semester_boundary(semester => $semester,
                                              tel => 'JCMT');

        # Use DateTime so that we can have proper addition. Add 1 year 1 day because
        # sem_end refers to the UT date and doesn't specify hours/minutes/seconds
        return DateTime->from_epoch(epoch => $sem_end->epoch,
                                    time_zone => 'UTC')
             + DateTime::Duration->new(years => 1, hours => 23,
                                       minutes => 59, seconds => 59);

    }
    else {
        # immediate release
        return DateTime::Format::ISO8601->parse_datetime($obsdate);
    }
}

# Ensure a date is in YYYYMMDD format.
sub _reformat_datetime {
    my $date = shift;

    throw JSA::Error::FatalError('date is undefined')
        unless defined $date;

    $date = DateTime::Format::ISO8601->parse_datetime($date)
        unless ref $date;

    return $date->ymd('');
}

# Check that the argument is a non-empty array reference.
# Returns any array reference without duplicates.
sub _unique_files {
    my $files = shift;

    throw JSA::Error 'files must be a non-empty array reference'
        unless $files && ref $files && scalar @$files;

    my %seen = ();

    return [grep {! $seen{$_} ++} @$files];
}

# Note: functions below were imported from the calcbounds script.

sub _calcbounds_check_hash_val {
    my ($href, $key, $check) = @_;

    return
        unless $href
            && $key
            && $check
            && exists $href->{$key};

    my $string = $href->{$key};

    return unless defined $string;

    # Compiled regex given.
    if (ref $check) {
        my ($found) = ($string =~ $check);
        return $found;
    }

    return $string if $string eq $check;

    return;
}

sub _calcbounds_any_header_sub_val {
    my ($header, $key, $check) = @_;

    my $found = _calcbounds_check_hash_val($header, $key, $check);
    return $found if $found;

    if (exists $header->{'SUBHEADERS'}) {
        for my $sh (@{$header->{'SUBHEADERS'}}) {
            $found = _calcbounds_check_hash_val($sh, $key, $check);
            return $found if $found;
        }
    }

    return;
}


1;

__END__

=back

=head1 NOTES

Skips any data files that are from simulated runs (SIMULATE=T).

=head1 AUTHORS

=over 2

=item *

Anubhav E<lt>a.agarwal@jach.hawaii.eduE<gt>

=item *

Kynan Delorey E<lt>k.delorey@jach.hawaii.eduE<gt>

=item *

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>

=back

Copyright (C) 2006-2014, Science and Technology Facilities Council.
Copyright (C) 2015 East Asian Observatory
All Rights Reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or (at
your option) any later version.

This program is distributed in the hope that it will be useful,but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place,Suite 330, Boston, MA  02111-1307,
USA

=cut
