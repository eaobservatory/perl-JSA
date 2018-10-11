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
use File::Basename;
use File::Temp;
use List::MoreUtils qw/any all/;
use List::Util qw/min max minstr maxstr/;
use Log::Log4perl;
use Scalar::Util qw/blessed looks_like_number/;

use Astro::Coords::Angle::Hour;

use JSA::DB;
use JSA::Headers qw/read_jcmtstate file_md5sum/;
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

    if (exists $args{'debug_fh'} and defined $args{'debug_fh'}) {
        $obj->{'_debug_fh'} = $args{'debug_fh'};
    }

    return $obj;
}

=item B<read_file_extra>

Returns undef -- subclasses should overrride this if extra information needs
to be read from raw files.

=cut

sub read_file_extra {
    my $self = shift;
    my $filename = shift;

    return undef;
}

=item construct_missing_headers($filename, $header, $extra)

Returns undef -- subclasses should overrride this if there are raw files
which lack important headers.

=cut

sub construct_missing_headers {
    my $self = shift;
    my $filename = shift;
    my $header = shift;
    my $extra = shift;

    return undef;
}

=item B<calc_freq>

Does nothing currently.

=cut

sub calc_freq {
  my ($self) = @_;

  return;
}

=item B<need_wcs>

Indicates whether we need WCS information.

=cut

sub need_wcs {
    return 0;
}

=item preprocess_header($filename, $header, $extra)

Perform any necessary header pre-processing.

This operates on Astro::FITS::Header objects.  Items are copied from
C<$extra> into the C<$header> object unless they already exist.
Items returned by C<construct_missing_headers> are also added.

Subclasses should any extra pre-processing required.

=cut

sub preprocess_header {
    my $self = shift;
    my $filename = shift;
    my $header = shift;
    my $extra = shift;

    foreach my $additional (
            $extra,
            $self->construct_missing_headers($filename, $header, $extra)) {
        if (defined $additional) {
            foreach my $item ($additional->allitems()) {
                my $keyword = $item->keyword();

                unless (defined $header->index($keyword)) {
                    $header->insert((scalar $header->allitems()), $item);
                }
            }
        }
    }
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
I<insert_observation> method) for a date or given list of files.

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

=item overwrite

Overwrite mode -- any values to be updated are overwritten rather
than adjusted allowing for incremental data entry.

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

=item from_mongodb

Read file information from MongoDB.

=back

=cut

sub prepare_and_insert {
    my ($self, %arg) = @_;

    my $log = Log::Log4perl->get_logger('');

    my $dry_run = $arg{'dry_run'};
    my $skip_state = $arg{'skip_state'};

    my $mdb = undef;
    if ($arg{'from_mongodb'}) {
        require JSA::DB::MongoDB;
        $mdb = new JSA::DB::MongoDB();
    }

    my %update_args = map {$_ => $arg{$_}} qw/
        calc_radec
        overwrite
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
    # for each observation.
    my ($obs_list, $wcs, $md5) = $self->_get_observations(
        dry_run => $dry_run,
        skip_state => $skip_state,
        mongodb => $mdb,
        %obs_args);

    unless ($obs_list and (ref $obs_list) and (scalar @$obs_list)) {
        $log->debug("No observations found for instrument $name");
        return;
    }

    $log->debug(
        (exists $obs_args{'date'})
        ? sprintf("Inserting data for %s. Date [%s]",
                  $name, $obs_args{'date'})
        : "Inserting given files");

    # The %columns hash will contain a key for each table, each key's value
    # being an anonymous hash containing the column information.

    my $db = new OMP::DBbackend::Archive();
    my $dbh = $db->handle_checked();

    my %columns = map {$_ => $self->get_columns($_, $dbh)}
        qw/COMMON FILES/, $self->instrument_table();

    foreach my $obs (@$obs_list) {
        $self->insert_observation(
            db => $db,
            columns => \%columns,
            observation => $obs,
            dry_run => $dry_run,
            skip_state => $skip_state,
            do_verification => (not defined $mdb),
            file_wcs => $wcs,
            file_md5 => $md5,
            %update_args);
    }
}

=item B<insert_observation>

For each observation:

=over 4

=item Insert a row in the COMMON table.

=item Insert a row in the [INSTRUMENT] table for each subsystem used.

=item Insert a row in the FILES table for each file.

=back

=cut

sub insert_observation {
    my ($self, %arg) = @_;

    my $log = Log::Log4perl->get_logger('');

    my ($db, $observation, $columns, $file_wcs, $file_md5, $overwrite, $dry_run, $skip_state) =
       map {$arg{$_}} qw/db observation columns file_wcs file_md5 overwrite dry_run skip_state/;

    my $dbh  = $db->handle();
    my %common_arg = map {$_ => $arg{$_}} qw/update_only_inbeam update_only_obstime/;
    my $update_only = grep {$_} values %common_arg;

    my @file = $observation->simple_filename();

    foreach (@file) {
        if (exists $self->{'_cache_touched'}->{$_}) {
            $log->debug("already processed: $_");

            return;
        }
    }

    $self->{'_cache_touched'}->{$_} = 1 foreach @file;

    # Extract information from the OMP::Info::Obs object.
    my @subsystems;
    my ($common_hdrs, $common_files);
    foreach my $subsystem ($observation->subsystems()) {
        # Break hash tie by copying & have an explicit anonymous hash ( "\%{ ... }"
        # does not untie).  This is so that a single element array reference when
        # assigned to one of the keys is assigned as reference (not as the element
        # contained with in).
        my $headers = {%{$subsystem->hdrhash}};

        $self->_fill_headers_obsid_subsys($headers, $subsystem->obsid);

        $self->munge_header_INBEAM($headers);

        $self->fill_max_subscan($headers);

        $self->fill_headers_FILES($headers, $subsystem, $file_md5);

        $self->calc_freq($subsystem, $headers, $file_wcs);

        if ($self->can('transform_header')) {
            $headers = $self->transform_header($headers);
        }

        if ($arg{'do_verification'} and $self->_do_verification()) {
            my $verify = JCMT::DataVerify->new(Obs => $subsystem);

            unless (defined $verify) {
                $log->logdie(join "\n",
                    'Could not make JCMT::DataVerify object:',
                    $subsystem->summary('text'),
                    Dumper([sort $subsystem->filename()]),
                );
            }

            my %invalid = $verify->verify_headers();

            foreach (keys %invalid) {
                my $val = $invalid{$_}->[0];

                if ($val =~ /does not match/i) {
                    $log->debug("$_ : $val");
                    undef $headers->{$_};
                }
                elsif ($val =~ /should not/i) {
                    $log->debug("$_ : $val");
                    undef $headers->{$_} if $headers->{$_} =~ /^UNDEF/ ;
                }
            }
        }

        push @subsystems, $headers;

        # If this was the first subsystem, use it to provide information for
        # the COMMON table.  Record the file names so that we can supply them
        # to "calc_radec" in the "try" block below.
        unless (defined $common_hdrs) {
            $common_hdrs = $headers;
            $common_files = [$subsystem->filename()];

            $self->fill_headers_COMMON($headers, $subsystem);
        }
    }

    unless (defined $common_hdrs) {
        $log->debug('First subsystem is undefined; nothing to do.');
        return;
    }

    $log->debug(sprintf "[%s]...", join ', ', @file);

    if (! $arg{'process_simulation'} && $self->is_simulation($common_hdrs)) {
        $log->debug("simulation data; skipping" );

        $self->_get_xfer_unconnected_dbh()->put_state(
                state => 'simulation', files => \@file,
                dry_run => $dry_run)
            unless $skip_state;

        return;
    }

    try {
        if ($arg{'calc_radec'}
                && ! $self->skip_calc_radec('headers' => $common_hdrs)) {

            unless ($self->calc_radec($common_hdrs, $common_files)) {
                $log->debug("problem while finding bounds; skipping");

                throw JSA::Error('could not find bounds');
            }
        }

        # COMMON table.
        $db->begin_trans() if not $dry_run;

        my $table = 'COMMON';

        $self->_update_or_insert(
            update_args => \%common_arg,
            dbhandle   => $dbh,
            table      => $table,
            table_columns => $columns->{$table},
            headers    => $common_hdrs,
            overwrite  => $overwrite,
            dry_run    => $dry_run);

        if ($dbh->err()) {
            my $text = $dbh->errstr();

            $db->rollback_trans();

            throw JSA::Error($text);
        }

        # FILES and instrument-specific tables.
        unless ($update_only) {
            $self->insert_subsystems(
                db  => $db,
                columns => $columns,
                subsystems => \@subsystems,
                overwrite => $overwrite,
                dry_run => $dry_run,
                skip_state => $skip_state);
        }

        $db->commit_trans() if not $dry_run;

        $log->debug("successful");
    }
    catch Error::Simple with {
        my $e = shift;
        my $text = $e->text();
        $log->error('error inserting obs set: ' . $text) if $text;
        $text = 'unknown error' unless $text;

        $self->_get_xfer_unconnected_dbh()->put_state(
                state => 'error', files => \@file,
                comment => $text, dry_run => $dry_run)
            unless $skip_state;
    };
}

=item B<_do_verification>

Should we use JCMT::DataVerify?

=cut

sub _do_verification {
    my $self = shift;
    return 1;
}

=item B<_get_observations>

Returns by reference an array of L<OMP::Info::Obs> objects and hashes of WCS
information, if available, and MD5 sums.

    ($obs, $wcs, $md5sum) = $enter->_get_observations(
        date => '20090609',
        dry_run => $dry_run,
        skip_state => $skip_state,
    );

Note: writes the file state in the transfer table unless the
dry_run argument is given, or using data from MongoDB.

=cut

sub _get_observations {
    my ($self, %args) = @_;
    my $dry_run = $args{'dry_run'};
    my $skip_state = $args{'skip_state'};
    my $mdb = $args{'mongodb'};

    my $log = Log::Log4perl->get_logger('');

    my @headers;
    my %wcs;
    my %md5;

    unless (defined $mdb) {
        my $xfer = $self->_get_xfer_unconnected_dbh();

        my @file;

        unless (exists $args{'files'}) {
            throw JSA::Error::FatalError('Neither files nor date given to _get_observations')
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
                        state => 'ignored', files => [$base], comment => $ignored,
                        dry_run => $dry_run)
                    unless $skip_state;

                $log->warn("$ignored: $file; skipped.\n");

                next;
            }

            $xfer->add_found(files => [$base], dry_run => $dry_run)
                unless $skip_state;

            my $text = '';
            my $err;

            my %info_obs_opt = (
                nocomments => 1,
                retainhdr  => 1,
                ignorebad  => 1,
                header_search => 'files'
            );

            try {
                my $obs;
                if ($file =~ /\.fits/) {
                    # Since the OMP can't read headers from FITS files,
                    # do it here.  This also allows us to deal with
                    # RxH3 data, since only it uses FITS format and the
                    # code to deal with its lack of metadata has been
                    # located in this package for now.
                    require Astro::FITS::Header::CFITSIO;
                    my $header = new Astro::FITS::Header::CFITSIO(File => $file, ReadOnly => 1);
                    my $extra = $self->read_file_extra($file);
                    $self->preprocess_header($file, $header, $extra);
                    $obs = new OMP::Info::Obs(
                        fits => $header, wcs => undef, %info_obs_opt);
                    $obs->filename($file);
                }
                else {
                    $obs = OMP::Info::Obs->readfile($file, %info_obs_opt);
                }
                push @obs, $obs;
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
                        state => 'error', files => [$base], comment => $text,
                        dry_run => $dry_run)
                    unless $skip_state;

                $log->error($text);
            }
        }

        for my $ob (@obs) {
            my $header = $ob->hdrhash;

            my $filename = $ob->filename();

            my $basename = _basename($filename);
            $wcs{$basename} = $ob->wcs() if $self->need_wcs();
            $md5{$basename} = file_md5sum($filename);

            push @headers, {
                filename => $filename,
                header => $header,
            };
        }
    }
    else {
        die 'Files specified for _get_observations in MonboDB mode'
            if exists $args{'files'};
        die 'Date not specified for _get_observations in MonboDB mode'
            unless exists $args{'date'};


        my $result = $mdb->get_raw_header(
            date => $args{'date'},
            instrument => $self->instrument_name(),
        );

        foreach my $entry (@$result) {
            my $file = $entry->{'file'};

            $wcs{$file} = $entry->{'wcs'};
            $md5{$file} = $entry->{'md5sum'};

            my $header = $entry->{'header'};

            $self->preprocess_header($file, $header, $entry->{'extra'});

            $header->tiereturnsref(1);
            tie my %header, ref($header), $header;

            push @headers, {
                filename => $file,
                header => \%header,
            };
        }
    }

    return unless scalar @headers;

    # The headers will be passed to OMP::FileUtils->merge_dupes which
    # in turn passes them to Astro::FITS::Header->new(Hash => ...).
    # That constructor drops any null or empty string headers.  Since
    # we need to see the INBEAM header for all files, replace blank
    # values with a dummy placeholder first.  (See also
    # munge_header_INBEAM where these placeholders are removed.)
    foreach my $entry (@headers) {
        my $header = $entry->{'header'};
        if (exists $header->{'INBEAM'}) {
            unless ((defined $header->{'INBEAM'})
                    and ($header->{'INBEAM'} ne '')) {
                $header->{'INBEAM'} = 'NOTHING';
            }
        }
    }


    my $merged = OMP::FileUtils->merge_dupes(@headers);

    my @obs = OMP::Info::Obs->hdrs_to_obs(
        retainhdr => 1,
        fits      => $merged);

    return (\@obs, \%wcs, \%md5);
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


=item B<insert_subsystems>

Adds observation subsystems into the instrument and FILES tables.

The observations array reference is for a given run number.

    $enter->insert_subsystems(
        db         => $db,
        columns    => \%cols,
        subsystems => \@headers_per_subsystem,
        dry_run    => $dry_run,
        skip_state => $skip_state);

It is called by the I<insert_observation> method.

=cut

sub insert_subsystems {
    my ($self, %args) = @_;

    my $log = Log::Log4perl->get_logger('');

    my ($db, $subsystems, $columns, $overwrite, $dry_run, $skip_state) =
        map {$args{$_}} qw/db subsystems columns overwrite dry_run skip_state/;

    my $dbh = $db->handle();

    my $subsysnr = 0;
    my $totsub = scalar @$subsystems;

    foreach my $subsys_hdrs (@$subsystems) {
        $subsysnr ++;
        $log->debug("Processing subsysnr $subsysnr of $totsub");

        do {
            my $table = 'FILES';

            $self->_change_FILES(
                headers        => $subsys_hdrs,
                db             => $db,
                table          => $table,
                table_columns  => $columns->{$table},
                dry_run        => $dry_run,
                skip_state     => $skip_state);
        };

        do {
            my $table = $self->instrument_table();

            $self->_update_or_insert(
                dbhandle  => $dbh,
                table     => $table,
                table_columns => $columns->{$table},
                headers   => $subsys_hdrs,
                overwrite => $overwrite,
                dry_run   => $dry_run);

            if ($dbh->err()) {
                my $text = $dbh->errstr();

                $db->rollback_trans() if not $dry_run;
                $log->debug("$text");

                throw JSA::Error($text);
            }
        };
    }
}

=item B<_expand_header_arrays>

Check whether a header contains array references.  If so, it is expanded
into a series of headers.  Each new header hash contains copies of the
single-valued entries and one value for each of the array entries.
Each array entry must contain the same number of elements as the generated
headers will contain the corresponding entries from each array.

=cut

sub _expand_header_arrays {
    my ($self, $vals) = @_;

    my $log = Log::Log4perl->get_logger('');

    # Go through the hash and check which entries have array references.
    my @array_keys;
    my $nrows = undef;
    while (my ($key, $val) = each %$vals) {
        my $ref = ref $val
            or next;

        $log->logdie("Unsupported reference type in insert hash!\n")
            unless $ref eq 'ARRAY';

        my $row_count = scalar @$val;
        unless (defined $nrows) {
            $nrows = $row_count;
        }
        elsif ($row_count != $nrows) {
            $log->logdie("Uneven row count in header hash ARRAY ref for key '$key'",
                         " ($row_count != $nrows) compared to first key '$array_keys[0]'\n")
        }

        push @array_keys, $key;
    }

    return [$vals] unless scalar @array_keys;

    # Now create an array of hashes with array references unrolled.
    my @headers = ();

    # Loop over the known number of rows.
    for (my $i = 0; $i < $nrows; $i ++) {
        push @headers, {%$vals, map {$_ => $vals->{$_}->[$i]} @array_keys};
    }

    return \@headers;
}

=item B<insert_hash>

Given a table name, a DBI database handle and a hash reference, insert
the hash contents into the table.

    $keys_inserted = $enter->insert_hash(
        table      => $table,
        dbhandle   => $dbh,
        insert     => \%to_insert
        dry_run    => $dry_run);

Returns (by reference) an array of the primary key values inserted.

=cut

sub insert_hash {
    my ($self, %args) = @_;

    my ($table, $dbh, $insert, $dry_run, $on_duplicate) =
        @args{qw/table dbhandle insert dry_run on_duplicate/};

    my $log = Log::Log4perl->get_logger('');

    # Get the fields in sorted order (so that we can match with values)
    # and create a template SQL statement. This can be done with the
    # first hash from @insert_hashes

    my @insert_hashes = @{$insert};

    my $prim_key =_get_primary_key($table);
    $log->logdie('Primary key not defined for table: ', $table)
        unless defined $prim_key;

    my @fields = sort keys %{$insert_hashes[0]}; # sort required

    my $sql = sprintf
        "INSERT INTO %s (%s) VALUES (%s)",
        $table,
        join(', ', @fields),
        join(', ', ('?') x scalar @fields);

    $sql .= ' ON DUPLICATE KEY UPDATE ' . $on_duplicate
        if defined $on_duplicate;

    my $sth;
    unless ($dry_run) {
        $sth = $dbh->prepare($sql)
            or $log->logdie(
                "Could not prepare SQL statement for insert\n",
                $dbh->errstr);
    }

    # and insert all the rows
    my @keys_inserted;
    for my $row (@insert_hashes) {
        my @values = @{$row}{@fields}; # hash slice

        $log->trace('-----> SQL: ' . $sql);
        $log->trace(Dumper(\@values));

        my $affected;
        unless ($dry_run) {
            $affected = $sth->execute(@values);
        }
        else {
            # In dry run mode, assume the one entry was inserted.
            $affected = 1;
        }

        if (defined $affected && $affected > 0) {
            push @keys_inserted, $row->{$prim_key};
        }
    }

    return \@keys_inserted;
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

Additional options:

=over 4

=item date_start

Date corresponding to the start of the data provided (DateTime object).

=item date_end

Date corresponding to the end of the data provided (DateTime object).

=item overwrite

If set to a true value, generate update operations which would simply
overwrite all values which differ from those currently in the database,
ignoring the usual update rules.

=back

=cut

sub prepare_update_hash {
    my ($self, $table, $dbh, $rows, %arg) = @_;

    my $log = Log::Log4perl->get_logger('');

    $log->logdie('prepare_update_hash cannot be used for table FILES')
        if $table eq 'FILES';

    # work out which key uniquely identifies the row
    my $unique_key = _get_primary_key($table);

    $log->logdie("No unique keys found for table name: '$table'\n")
        unless defined $unique_key;

    my $sql = "select * from $table where $unique_key = ?";

    my (%start, %end);

    if ($table eq 'COMMON') {
        %start = map {$_ => undef} JSA::DB::TableCOMMON::range_start_columns();
        %end = map {$_ => undef} JSA::DB::TableCOMMON::range_end_columns();
    }

    my $is_overwrite = $arg{'overwrite'};

    my @update_hash;
    my @insert_hash;
    foreach my $row (@$rows) {
        my $unique_val = $row->{$unique_key};

        $log->trace(Dumper([$unique_key, $unique_val]));

        my $ref = $dbh->selectall_arrayref($sql, {Columns=>{}}, $unique_val)
            or $log->logdie("Error retrieving existing content using [$sql]: ", $dbh->errstr, "\n");

        $log->logdie("Only retrieved partial dataset: ", $dbh->errstr, "\n")
            if $dbh->err;

        # how many rows
        my $count = scalar @$ref;

        if (0 == $count) {
            # Row does not already exist: add to the insert list.
            $log->debug("new data to insert: " . $unique_val);

            push @insert_hash, $row;

            next;
        }

        $log->logdie("Should not be possible to have more than one row. Got $count\n")
            if $count > 1;

        my $indb = $ref->[0];

        # Determine whether we are handling data pertaining to the start and/or
        # end of the observation.  Note that we can only do this for the COMMON
        # table, since only it has date_obs and date_end values, but it is also
        # the only table which currently has range values.
        my $is_start = 1;
        my $is_end = 1;
        if (defined $arg{'date_start'}
                and exists $indb->{'date_obs'}
                and defined $indb->{'date_obs'}) {
            my $db_date_start = make_datetime($indb->{'date_obs'});
            $is_start = 0 unless $arg{'date_start'} <= $db_date_start;
            $log->debug(sprintf 'This data %s the start of the observation',
                $is_start ? 'covers' : 'does not cover');
        }
        if (defined $arg{'date_end'}
                and exists $indb->{'date_end'}
                and defined $indb->{'date_end'}) {
            my $db_date_end = make_datetime($indb->{'date_end'});
            $is_end = 0 unless $arg{'date_end'} >= $db_date_end;
            $log->debug(sprintf 'This data %s the end of the observation',
                $is_end ? 'covers' : 'does not cover');
        }

        my %differ = ();
        my $ymd_start = qr/^\d{4}-\d{2}-\d{2}/;

        foreach my $key (sort keys %$row) {
            $log->trace("testing field: $key");

            my $new = $row->{$key};
            my $old = $indb->{$key};

            next unless (defined $old || defined $new);

            $log->trace("continuing with $key");

            # Temporarily list columns where we always want to keep
            # the maximum value here.
            my $keep_max = grep {$_ eq $key} qw/max_subscan/;

            # Skip pre-defined-check tests in overwrite mode.
            unless ($is_overwrite) {
                # INBEAM header: special handling.
                if ($key eq 'inbeam') {
                    my $combined = $self->_combine_inbeam_values($old, $new);
                    $differ{$key} = $combined;
                    $log->trace("$key = " . ($combined // '<undef>') . ' (combined inbeam)');
                    next;
                }
            }

            # Not defined currently - inserting new value.
            if (defined $new && ! defined $old) {
                $differ{$key} = $new;
                $log->trace("$key = $new");
                next;
            }

            # Defined in DB but undef in new version - not expecting this but assume
            # this means a null.
            if (! defined $new && defined $old) {
                $differ{$key} = undef;
                $log->trace("$key = <undef>");
                next;
            }

            # Determine if the value has changed: skip this key otherwise.
            if ($new =~ $ymd_start and $old =~ $ymd_start) {
                if (make_datetime($new) == make_datetime($old)) {
                    $log->trace("$key <skipped> (date unchanged)");
                    next;
                }
            }
            elsif (looks_like_number($new)) {
                if ($new =~ /\./) {
                    # floating point
                    my $diff = abs($old - $new);
                    unless ($diff > 0.000001) {
                        $log->trace("$key <skipped> (float unchanged)");
                        next;
                    }
                }
                elsif ($new == $old) {
                    $log->trace("$key <skipped> (number unchanged)");
                    next;
                }
            }
            elsif ($new eq $old) {
                $log->trace("$key <skipped> (string unchanged)");
                next;
            }

            # Overwrite mode?  If so update unconditionally.
            if ($is_overwrite) {
                $differ{$key} = $new;
                $log->trace("$key = $new (overwrite mode)");
                next;
            }

            # Range values.
            if (exists $start{$key}) {
                if ($is_start) {
                    $differ{$key} = $new;
                    $log->trace("$key = $new (is start)");
                }
                else {
                    $log->trace("$key <skipped> (not start)");
                }
                next;
            }

            if (exists $end{$key}) {
                if ($is_end) {
                    $differ{$key} = $new;
                    $log->trace("$key = $new (is end)");
                }
                else {
                    $log->trace("$key <skipped> (not end)");
                }
                next;
            }

            # Maximum value.
            if ($keep_max) {
                if ($new > $old) {
                    $differ{$key} = $new;
                    $log->trace("$key = " . $new . " (new maximum)");
                }
                else {
                    $log->trace("$key <skipped> (not larger)");
                }
                next;
            }

            # Any other value (number of string).
            $differ{$key} = $new;
            $log->trace("$key = $new (changed)");
        }

        $log->debug("differences to update: " . (join ' ', keys %differ));

        push @update_hash, {
            differ        => \%differ,
            unique_val    => $unique_val,
            unique_key    => $unique_key,
        } if scalar %differ;
    }

    return (\@update_hash, \@insert_hash);
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
        FILES     => 'file_id',
        RXH3      => 'obsid_subsysnr',
        SCUBA2    => 'obsid_subsysnr',
        transfer  => 'file_id',
    );

    return unless exists $keys{$table};
    return $keys{$table};
}

=item B<update_hash>

Given a table name, a DBI database handle and a hash reference,
update the values specified.

    $enter->update_hash($table, $dbh, \@to_update, dry_run => $dry_run);

The changes to be performed should be passed as a list of hashes
of the form returned by C<prepare_update_hash>.

=cut

sub update_hash {
    my ($self, $table, $dbh, $changes, %args) = @_;

    my $dry_run = $args{'dry_run'};

    my $log = Log::Log4perl->get_logger('');

    foreach my $row (@$changes) {
        my @sorted = sort keys %{$row->{'differ'}};
        next unless scalar @sorted;

        my $sql = sprintf(
            "UPDATE %s SET %s WHERE %s = ?",
            $table,
            (join ', ', map {" $_ = ? "} @sorted),
            $row->{'unique_key'});

        my @bind = map {$row->{'differ'}{$_}} @sorted;
        push @bind, $row->{'unique_val'};

        $log->trace($sql);
        $log->trace(Dumper(\@bind));

        next if $dry_run;

        my $sth = $dbh->prepare($sql)
            or $log->logdie("Could not prepare sql statement for UPDATE\n", $dbh->errstr, "\n");

        $sth->execute(@bind);
        throw JSA::Error::DBError 'UPDATE error: ' . $dbh->errstr() . "\n... with { $sql, @bind }"
            if $dbh->err();
    }
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

    $enter->fill_headers_FILES(\%header, $obs, $file_md5);

=cut

sub fill_headers_FILES {
    my ($self, $header, $obs, $file_md5) = @_;

    my $log = Log::Log4perl->get_logger('');

    # Create file_id - also need to extract NSUBSCAN from subheader if we have more
    # than one file. (although simply using a 1-based index would be sufficient)
    my @files = $obs->simple_filename;
    $header->{'file_id'} = \@files;

    # Create "md5sum" header by extracting values from the given hash.
    $header->{'md5sum'} = [map {$file_md5->{$_}} @files];

    # We need to know whether a nsubscan header is even required so %columns really
    # needs to be accessed. For now we kluge it.
    unless (exists $header->{'nsubscan'}) {
        if (exists $header->{'NSUBSCAN'}) {
            $log->debug('fill_headers_FILES: NSUBSCAN already present at top level');

            # not really needed because the key becomes case insensitive
            $header->{'nsubscan'} = $header->{'NSUBSCAN'};
        }
        else {
            $log->debug('fill_headers_FILES: NSUBSCAN not present at top level');

            # We need to get the NSUBSCAN values from the subheaders.  We can't
            # use $self->_find_header because we need to retain the ordering.
            my @subscans = map {$_->{'NSUBSCAN'}} @{$header->{'SUBHEADERS'}};

            $log->logdie('fill_headers_FILES: number of NSUBSCAN headers does not match number of files')
                unless (scalar @files) == (scalar @subscans);

            $header->{'nsubscan'} = \@subscans;
        }
    }

    $log->trace(sprintf
        "Created header [file_id] with value [%s]",
        join ',', @{$header->{'file_id'}});

    return;
}


=item B<munge_header_INBEAM>

Given a header hash reference, removes all the I<INBEAM> header occurrences
which have C<SHUTTER>; combines any remaining header values (in subheaders) in a
space separated list.

    $enter->munge_header_INBEAM($header_hash);

=cut

sub munge_header_INBEAM {
    my ($self , $headers) = @_;

    my $name = 'INBEAM';

    # Find INBEAM values, but remove dummy placeholder values.
    # (See also _get_observations where these are inserted.)
    my @val = map {($_ eq 'NOTHING') ? undef : $_} $self->_find_header(
        headers => $headers,
        name   => $name,
        value  => 1,
    );

    $headers->{$name} = (scalar @val)
        ? $self->_combine_inbeam_values(@val)
        : undef;
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

=item B<fill_max_subscan>

Fills in the I<max_subscan> value, given a
headers hash reference.

    $inst->fill_max_subscan(\%header);

=cut

sub fill_max_subscan {
    my ($self, $header) = @_;

    $header->{'max_subscan'} = max $self->_find_header(
        headers => $header,
        name => 'NSUBSCAN',
        value => 1);
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

    $vals = $enter->get_insert_values($table, \%table_columns, \%headers, %args);

Additional arguments may be given in hash form:

=over 4

=item update_only_inbeam

Only include the C<INBEAM> header value.

=item update_only_obstime

Only include the times for an observation.

=item update_only_obsradec

Only include obsra, obsdec and their associated tl, tr, bl, br values.

=back

=cut

sub get_insert_values {
    my ($self, $table, $table_columns, $headers, %args) = @_;

    my $only_obstime = $args{'update_only_obstime'};
    my $only_inbeam = $args{'update_only_inbeam'};
    my $only_obsradec = $args{'update_only_obsradec'};

    my $obs_date_re = qr/^date.(?:obs|end)$/i;
    my $inbeam_re = qr/^inbeam$/i;
    my $obsradec_re = qr/^obs(?:ra|dec)/i;

    my $log = Log::Log4perl->get_logger('');

    my $dict = $self->get_dictionary();
    my $unique_key = _get_primary_key($table);

    # Map headers to columns, translating from the dictionary as
    # necessary.
    my %values;

    foreach my $header (sort {lc $a cmp lc $b} keys %$headers) {
        my $key = lc $header;

        if (exists $table_columns->{$key}) {
            # Use key name directly.
        }
        elsif (exists $dict->{$key}
                && exists $table_columns->{$dict->{$key}}) {
            # Found header alias in dictionary and column exists in table
            $key = $dict->{$key};

            $log->trace("  MAPPED header [$header] to column [$key]");
        }
        else {
            $log->trace("  Could not find alias for header [$header].  Skipped.");
            next;
        }

        if ($key eq $unique_key) {
            # Always include primary key to identify record.
        }
        elsif (($only_inbeam && $key !~ $inbeam_re)
                or ($only_obstime && $key !~ $obs_date_re)
                or ($only_obsradec && $key !~ $obsradec_re)) {
            $log->trace("  Skipping field: $key (due to field restriction)");
            next;
        }

        $values{$key} = $headers->{$header};
    }

    # Do value transformation
    $self->transform_value($table_columns, \%values);

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

=item B<skip_calc_radec>

Given a object header hash reference, returns a true value if
bounding box calculation should be skipped.

    print "skipped calc_radec()"
        if $enter->skip_calc_radec(headers => $obs->hdrhash());

The skip test is:

    'OBS_TYPE' => qr/\b skydips? \b/ix

=cut

sub skip_calc_radec {
    my ($self, %arg) = @_;

    throw JSA::Error::BadArgs('No "headers" value given to check if to find bounding box.')
        unless defined $arg{'headers'};

    return $self->_find_header(
        headers => $arg{'headers'},
        name    => 'OBS_TYPE',
        value_regex => qr/\b skydips? \b/xi)
}

=item B<calc_radec>

Calculate RA/Dec extent (ICRS) of the observation and the base
position.  It populates header with corners of grid (in decimal
degrees).  Status is perl status: 1 is good, 0 bad.

    $status = JSA::EnterData->calc_radec($header, \@filenames);

=cut

sub calc_radec {
    my ($self, $headerref, $filenames) = @_;

    my $log = Log::Log4perl->get_logger('');

    # Now need to write these files to a temp file
    my $temp = File::Temp->new(template => '/tmp/radec-XXXXXXXXXX');
    $temp->unlink_on_destroy(1);
    write_list($temp->filename(), $filenames);

    # PA (may not be present)
    my $pa = $headerref->{'MAP_PA'};
    $pa *= -1 if defined $pa;

    my @command  = $self->get_bound_check_command($temp->filename(), $pa);

    $log->info(sprintf(
        "Performing bound calculation for files starting %s", $filenames->[0]));

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
        %state = read_jcmtstate($filenames->[0], 'start', qw/TCS_TR_SYS/);
        $log->logdie(sprintf("Error reading state information from file %s", $filenames->[0]))
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

    my ($headers, $db, $table, $table_columns, $dry_run, $skip_state) =
        @arg{qw/headers db table table_columns dry_run skip_state/};

    throw JSA::Error('_change_FILES: columns not defined')
        unless defined $table_columns;

    $log->trace(">Processing table: $table");

    my $dbh = $db->handle();

    my $insert_ref = $self->get_insert_values($table, $table_columns, $headers);

    my ($files, $error);
    try {
        throw JSA::Error "Empty hash reference in _change_FILES."
            unless scalar keys %$insert_ref;

        _verify_file_name($insert_ref->{'file_id'});

        my $hash = $self->_expand_header_arrays($insert_ref);

        if (exists $self->{'_debug_fh'}) {
            $self->{'_debug_fh'}->print(Data::Dumper->Dump([$hash], ['FILES']));
        }

        $files = $self->insert_hash(
            table     => $table,
            dbhandle  => $dbh,
            insert    => $hash,
            dry_run   => $dry_run,
            on_duplicate => 'md5sum=md5sum');  # Should do nothing

        $error = $dbh->errstr
            if $dbh->err();
    }
    catch JSA::Error with {
        $error = shift @_;
    };

    $log->debug($error) if defined $error;

    if ( $dbh->err() ) {
        $db->rollback_trans() if not $dry_run;

        return;
    }

    if ((not $skip_state) and $files and scalar @{$files}) {
        my $xfer = $self->_get_xfer_unconnected_dbh();
        $xfer->put_state(
            state => 'ingested', files => $files,
            dry_run => $dry_run);
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

Additional options:

=over 4

=item update_args

Hash of field restiction options (passed to L<get_insert_values>).

If any "update_only" arguments are given with true values then
no insert operations are performed.

=item dry_run

Dry-run mode.

=item overwrite

Overwrite mode (passed to L<prepare_update_hash>).

=back

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

    # This function is currently only used for tables where we should only
    # have a single set of values to insert, but take the min/max values in
    # case it is ever used for the FILES table.  Assume that values for each
    # of the date headers are in the same format so we can just take the min
    # or max value by string comparison.  Extract now (rather than on the
    # output of "get_insert_values") in case tables other than COMMON have
    # range headers added to them in future.
    my $date_start = minstr $self->_find_header(
        headers => $headers, name => 'DATE-OBS', value => 1);
    my $date_end = maxstr $self->_find_header(
        headers => $headers, name => 'DATE-END', value => 1);

    # Do any "update_only_XXX" arguments have a true value?
    my $update_args = $args{'update_args'} // {};
    my $update_only = grep {/^update_only/ and $update_args->{$_}} keys %$update_args;

    my $vals = $self->get_insert_values($table, $table_columns, $headers, %$update_args);

    my $rows = $self->_expand_header_arrays($vals);

    if (exists $self->{'_debug_fh'}) {
        $self->{'_debug_fh'}->print(Data::Dumper->Dump([$rows], ["${table}_values"]));
    }

    my ($change_update, $change_insert) = $self->prepare_update_hash(
        @args{qw/table dbhandle/}, $rows,
        date_start => make_datetime($date_start),
        date_end => make_datetime($date_end),
        overwrite => $args{'overwrite'});

    if (exists $self->{'_debug_fh'}) {
        $self->{'_debug_fh'}->print(Data::Dumper->Dump(
            [$change_insert, $change_update],
            ["${table}_insert", "${table}_update"]));
    }

    if ((not $update_only) and scalar @$change_insert) {
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

    return $vals unless ref $vals eq 'ARRAY'
                     && 1 < scalar @{$vals};

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

Undefined values are not included.

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
    my ($head, $name, $val_re, $test) =
        @args{qw/headers name value_regex test/};
    my $return_value = (exists $args{'value'});

    $val_re = qr/\b${val_re}\b/x if defined $val_re && ! ref $val_re;

    my @ans = ();
    my $subhead = undef;

    # Do we have a single header, or an array of header hashes?
    unless ('ARRAY' eq ref $head) {
        my $subh = 'SUBHEADERS';
        if (exists $head->{$subh}) {
            $subhead = $head->{$subh};
        }

        $head = [$head];
    }

    # Check each given header hash.
    foreach my $h (@$head) {
        next unless exists $h->{$name};
        my $val = $h->{$name};

        # Note: will continue searching subheader values if test fails.
        if (defined $test) {
            next if $test eq 'true' && ! $val;
            next if $test eq 'defined' && ! defined $val;
        }

        # Note: returns answer based on whether first found header matches.
        if (defined $val_re) {
            next unless defined $val;
            return $val =~ $val_re;
        }

        if ($return_value) {
            next unless defined $val;
            push @ans, $val;
        }
        else {
            push @ans, 1;
        }
    }

    # If an answer was found, return it, otherwise recurse to subheaders.
    if (scalar @ans) {
        if (wantarray) {
            my %seen;
            return grep {! $seen{$_} ++} @ans;
        }
        else {
            return $ans[0];
        }
    }
    elsif (defined $subhead) {
        return $self->_find_header(%args, 'headers' => $subhead)
    }

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

sub _basename {
    return unless scalar @_;

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

    my $log = Log::Log4perl->get_logger('');

    my %obs_args = ();
    $obs_args{'date'} = _reformat_datetime($arg{'date'}) if exists $arg{'date'};
    $obs_args{'files'} = _unique_files($arg{'files'}) if exists $arg{'files'};

    my $n_err = 0;

    my $process_obs_re = join '|', @$obs_types;
       $process_obs_re = qr{\b( $process_obs_re )}xi;

    my ($obs_list, undef, undef) = $self->_get_observations(
            dry_run => $dry_run,
            skip_state => ($skip_state or $skip_state_found),
            monbodb => undef,
            %obs_args);

    unless ($obs_list and (ref $obs_list) and (scalar @$obs_list)) {
        $log->warn('Could not make obs list.');
        return;
    }

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
            # Retrieve all of the SEQ_TYPE values and check each of them,
            # because _find_header(value_regex => ...) will return false
            # as soon as it finds a header that doesn't match.
            unless (scalar grep {$_ eq $found_type} $self->_find_header(
                        headers => \%header, name => 'SEQ_TYPE', value => 1)) {
                $log->debug('  skipped uninteresting SEQ_TYPE');
                next;
            }
        }

        _fix_dates(\%header);

        $log->debug('  calculating bounds');

        unless ($self->calc_radec(\%header, [$common->filename()])) {
            $log->error('  ERROR  while finding bounds');

            unless ($skip_state) {
                $log->debug('Setting file paths with error state');
                my $xfer = $self->_get_xfer_unconnected_dbh();
                $xfer->put_state(
                        state => 'error', files => \@file_id,
                        comment => 'bound calc',
                        dry_run => $dry_run);
            }

            $n_err ++;
            next;
        }

        unless (any {exists $header{$_}} @bound) {
            $log->warn('  did not find any bound values.');
            return;
        }

        $log->info('  UPDATING headers with bounds');

        $self->_update_or_insert(
            %pass,
            headers     => \%header,
            dry_run     => $dry_run,
            overwrite   => 1,
            update_args => {update_only_obsradec => 1});
    }

    return $n_err;
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
