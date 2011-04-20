package JSA::EnterData;

=head1 NAME

JAS::EnterData - Parse headers and store in database

=head1 SYNOPSIS

  # Create new object, with specific header dictionary.
  my $enter = JAS::EnterData
              ->new( 'dict' => '/path/to/dict' );

  # Do not actually insert into database.
  #$enter->debug( 1 );

  # Upload metadata for Jun 25, 2008.
  $enter->date( 20080625 );

  # Fill metadata.
  $enter->update_mode( 0 );

  $enter->prepare_and_insert;

  # Fill file information.
  $enter->update_mode( 1 );

  $enter->prepare_and_insert;

=head1 DESCRIPTION

JAS::EnterData is a object oriented module to provide back end support
to load data to CADC.

Reads the headers of all data from either the current date or the
specified UT date, and uploads the results to the header database. If
no date is supplied the current localtime is used to determine the
relevant UT date (which means that it will still pick up last night's
data even if run after 2pm).

=head2 METHODS

=over 2

=cut

use strict;
use warnings;
use FindBin;

use File::Temp;
use List::MoreUtils qw[ any all ];
use List::Util qw[ min max ];
use Log::Log4perl;
use Scalar::Util qw[ blessed looks_like_number ];

use Astro::Coords::Angle::Hour;

use JSA::Headers qw/ read_jcmtstate read_wcs /;
use JSA::Datetime qw[ make_datetime ];
use JSA::DB::TableCOMMON;
use JSA::EnterData::ACSIS;
use JSA::EnterData::SCUBA2;
use JSA::Error qw[ :try ];
use JSA::Files qw[ looks_like_rawfile ];
use JSA::Starlink qw[ run_star_command ];
use JSA::DB::TableTransfer;
use JCMT::DataVerify;

use OMP::ArchiveDB;
use OMP::DBbackend::Archive;
use OMP::Info::ObsGroup;
use OMP::DateTools;
use OMP::General;

use DateTime;
use DateTime::Format::ISO8601;

use NDF;

$| = 1; # Make unbuffered

BEGIN {

  # Make sure that bad status from SMURF triggers bad exit status
  $ENV{ADAM_EXIT} = 1;
}

{
  my %default =
    (
      'date'               => undef,
      'sybase-date-format' => '%Y-%m-%d %H:%M:%S',

      'load-header-db' => 1,

      # $OMP::ArchiveDB::SkipDBLookup is changed.
      'force-disk'     => 1,
      'force-db'       => 0,

      # Only table update (no insert).
      'update-mode'    => 0,

      'instruments' =>
        [ JSA::EnterData::ACSIS->new,
          JSA::EnterData::SCUBA2->new,
        ],

      # Location of data dictionary
      # NOTE: This should go in to the OMP config system at some point
      'dict' =>
        '/jac_sw/archiving/jcmt/import/data.dictionary',
        #'/home/agarwal/src/jac-git/archiving/jcmt/import/data.dictionary',

      # To make OMP::Info::Obs out of given files.
      'files' => [],

      # If false, then nothing is printed (other than fatal messages).
      # Else, messages are printed with increasing verbosity.
      'verbosity' => 1,

      # Debugging.  Set to true to turn on.  Debugging means interact
      # with the database, but don't actually do any inserts, and be
      # very verbose.
      'debug' => 0,

      # Force procssing of simulation if true.
      'process_simulation' => 0,

      # Update only an observation run time.
      'obstime-only' => 0,

      # To avoid getting file paths with 'f'ound state from database.
      'path-not-from-db' => 0
    );

  #  Generate some accessor functions.
  for my $k ( keys %default, qw[ conditional_insert ] ) {

    next
      if any { $k eq $_ }
          (
            # Must be given in constructor.
            'dict',
            'obstime-only',
            # Special handling when date to set is given.
            'date',
            # Validate instruments before setting.
            'instruments',
            # Need to check for an array ref.
            'files',
            # Need to check for an array ref.
            'files',
          )
      # Need to turn off the other if one is true.
      or $k =~ m/^ force-d(?: isk | b ) $/x
      ;

    {
      (my $sub = $k ) =~ tr/-/_/;
      no strict 'refs';
      *$sub = sub {
                my $self = shift;

                return $self->{ $k } unless scalar @_;

                $self->{ $k } = shift;
                return;
              };
    }
  }

=item B<new>

Constructor.  A data dictionary file name is required, which is set by
default.  It can be overridden as ...

  $enter = JSA::EnterData->new( 'dict' => '/file/path/' );

Configuration values which can be passed as key-value pairs are ...

=over 4

=item I<date> C<yyyymmdd>

Date to set given in C<yyyymmdd> format.
Default is the current date in local timezone (at the time of creation
of the object).

=item I<debug> C<1 | 0>

Debugging means interact with the database, but don't actually do any
inserts, and be very verbose.  Default is false.

=item I<dict> C<file name>

File name for data dictionary.  Default name is in
"/import/data.dictionary" located in directory containing this module.

=item I<force-disk> C<1 | 0>

A truth value whether to force looking for data on disk, not in
database.  Default is true.

When the value is true, I<force-db> is marked false.

=item I<force-db> C<1 | 0>

A truth value whether to force looking for data in database, not on
disk. Default is false.

Currently it does not do anything.

When the value is true, I<force-disk> is marked false.

=item I<verbosity> C< 0 | 1 | 2 | 3 ... >

An integer value indicating message verbosity.

=item I<load-header-db> C<1 | 0>

A truth value to control loading of header database.  Default is true.

=item I<syb-date> C<date format>

Date format for Sybase database.  Default is C<%Y-%m-%d %H:%M:%S>.

=item I<update-mode> C<1 | 0>

A truth value to determine if to do database update (when true; see
I<update_hash>) or an insert (when false; see I<insert_hash>).

In insert mode, nothing is inserted in "FILES" table.

=back

=cut

  sub new {

    my ( $class, %args ) = @_;

    my $obj = bless { %default, %args }, $class;

    # Sanity checks.
    $obj->_verify_dict;

    for ( qw[ date force-db force-disk ] ) {

      ( my $sub = $_ ) =~ tr/-/_/;

      die "None such sub: $sub"
        unless $obj->can( $sub );

      $obj->$sub( $obj->$sub );
    }

    if ( $obj->debug ) {

      $_->debug( 1 ) for $obj->instruments;
    }

    $obj->skip_state_setting( 0 );

    return $obj;
  }

}

=item B<instruments>

Returns a list of instrument objects when no arguments given.  Else,
the list of given instrument objects is accepted for further use.

  # Currently set.
  $instruments = $enter->instruments;

  # Set ACSIS as the only instrument.
  $enter->instruments( JSA::EnterData::ACSIS->new );

=cut

sub instruments {

  my $self = shift;

  unless ( scalar @_ ) {

    my $inst = $self->{'instruments'};
    return
      defined $inst ? @{ $inst } : () ;
  }

  for my $inst ( @_ ) {

    throw JSA::Error "Instrument '$inst' is unknown."
      unless any { blessed $_ eq blessed $inst }
                  ( JSA::EnterData::ACSIS->new,
                    JSA::EnterData::SCUBA2->new
                  ) ;
  }

  $self->{'instruments'} = [ @_ ];

  return;
}

=item B<debug>

Returns the set truth value if no arguments given.

  $debug = $enter->debug;

Else, sets the value to turn on or off debugging; returns nothing.

  $enter->debug( 0 );

=item B<date>

Returns the set date if no arguments given.

  $date = $enter->date;

Else, sets the date to date given as L<DateTime> object or as a string
in C<yyyymmdd> format; returns nothing.  If date does not match
expected type, then current date in local timezone is used.

  $enter->date( '20251013' );

=cut

sub date {

  my $self = shift;

  return $self->{'date'} unless scalar @_;

  my $date = shift;

  if ( ! $date
      || ( ! ref $date && $date !~ /^\d{8}$/ )
      || ( ref $date && ! $date->isa( 'DateTime' ) )
     ) {

    $date = DateTime->now( 'time_zone' => '-1200' ) ;
  }
  elsif ( ! ref $date ) {

    $date = DateTime::Format::ISO8601->parse_datetime( $date );
  }

  $self->{'date'} = $date;

  return;
}

=item B<sybase_date_format>

Returns the set date format for Sybase database consumption if no
arguments given.

  $format = $enter->sybase_date_format;

Else, sets the date format; returns nothing.

  $enter->sybase_date_format( '%Y-%m-%d %H:%M:%S' );

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
    ! ( $OMP::ArchiveDB::FallbackToFiles && $OMP::ArchiveDB::SkipDBLookup )
      unless scalar @_;

  my ( $force ) = @_;

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

  $enter->force_disk( 1 );

=cut

sub force_disk {

  my $self = shift;

  return
    $OMP::ArchiveDB::FallbackToFiles && $OMP::ArchiveDB::SkipDBLookup
      unless scalar @_;

  my ( $force ) = @_;

  # Force observation queries to query files on disk rather than the database.
  $OMP::ArchiveDB::FallbackToFiles =
  $OMP::ArchiveDB::SkipDBLookup = !! $force;

  return;
}

=item B<path_not_from_db>

When called without a truth value, returns a truth value to indicate
if to avoid database to fetch raw file paths to ingest.

  print "Getting paths from database"
    unless $enter->path_not_from_db();

Else, sets the truth value to inidicate to access database.

  # Avoid database access.
  $enter->path_not_from_db( 1 );

=item B<load_header_db>

Returns the set truth value to indicate where to load in the header
database, if no arguments given.

  $load = $enter->load_header_db;

Else, sets the truth value to turn on or off loading the header
database; returns nothing.

  $enter->load_header_db( 1 );

=item B<update_mode>

Returns the set truth value if no arguments given.

  $update = $enter->update_mode;

Else, sets the value to turn on or off update (off or on insert);
returns nothing.  In insert mode, nothing is inserted in "FILES"
table.

  $enter->update_mode( 0 );

=item B<verbosity>

Returns integer value to indicate message verbosity, if no arguments
are given.

  $ok = $enter->verbosity;

Else, sets the value for later use; returns nothing.

  # Silence messages.
  $enter->verbosity( 0 );

=item B<get_dict>

Returns the file name for the data dictionary.

  $dict_file = $enter->get_dict;

=cut

sub get_dict {

  my ( $self ) = @_;
  return $self->{'dict'};
}

=item B<files>

If no array reference is given, returns the array reference of file
names.

  $files = $enter->files();

Else, saves the given array reference  & returns nothing.

  $enter->files( [ file_list() ] );

Throws I<JSA::Error> exception if the given argument is not an array
reference, or is empty.

=cut

sub files {

  my $self = shift @_;

  return $self->{'files'}
    unless scalar @_;

  my ( $files ) = @_;

  throw JSA::Error 'Need a non-empty array reference.'
    unless $files && ref $files && scalar @{ $files };

  my %seen;
  my $old = $self->{'files'};
  $self->{'files'} =
    [ grep
      { ! $seen{ $_ }++ }
      ( ( $old && ref $old ? @{ $old } : () ),
        @{ $files }
      )
    ];

  return;
}

=item B<files_given>

Returns a truth value to indicate if any files were provided to process.

  $use_date = ! $enter->files_given();

=cut

sub files_given {

  my ( $self ) = @_;

  my $files = $self->files;
  return  !! ( $files && scalar @{ $files } );
}

=item B<process_simulation>

Returns the set truth value to indicate whether simulation processing
is forced, if no arguments given.

  $skip_sim = ! $enter->process_simulation;

Else, sets the truth value to turn on or off simulation processing;
returns nothing.

  $enter->process_simulation( 1 );

=item B<prepare_and_insert>

Inserts observation in database retrieved from disk (see also
I<insert_observations> method) for a date (see also I<date> method).

Date can be given to the method, or can be set via I<new()> or
I<date()> method.  Current date is used if no date has been explicitly
set.  File paths are fetched from database unless otherwise specified.

  # Insert either for the set or current date, get file paths from
  # database.
  $enter->prepare_and_insert;

  # Insert for Jun 25, 2008.
  $enter->prepare_and_insert( 'date' => '20080625' );

  $enter->prepare_and_insert();

=cut

{
  # To keep track of already processed files.
  my ( $old_date , %touched );

  sub prepare_and_insert {

    my ( $self, %arg ) = @_;

    my ( $date ) = @arg{qw[ date ]};

    # Format date first before getting it back.
    $self->date( $date ) if defined $date ;
    $date = $self->date;

    if ( defined $old_date && $date->ymd ne $old_date->ymd ) {

      $self->_print_text( "clearing file cache\n" )
        if 1 < $self->verbosity;

      undef %touched;
    }

    $old_date = $date;

    # Tables of interest.  All instruments reference the COMMON table, so it is
    # first on the array.  Actual instrument table will be the second element.
    my @tables = qw/COMMON/;

    my $db = OMP::DBbackend::Archive->new;
    my $dbh = $db->handle;

    $dbh->{'syb_show_eed'} = $dbh->{'syb_show_sql'} = 1;

    # The %columns hash will contain a key for each table, each key's value
    # being an anonymous hash containing the column information.  Store this
    # information for the COMMON table initially.
    my $columns;
    $columns->{$tables[0]} = $self->get_columns( $tables[0], $dbh );

    my %dict = $self->create_dictionary;

    my ( $observations, $group, $name, @files_added );

    for my $inst ( $self->instruments ) {

      $name = $inst->name;

      # Retrieve observations from disk.  An Info::Obs object will be returned
      # for each subscan in the observation.  No need to retrieve associated
      # obslog comments. That's <no. of subsystems used> *
      # <no. of subscans objects returned per observation>.
      $group = $self->_get_obs_group( 'name' => $name,
                                      'date' => $date,
                                    );
      my @obs =
        $self->_filter_header( $inst,
                              [ $group->obs ],
                              'OBS_TYPE' => [qw[ FLATFIELD ] ],
                          );

      $self->_print_text( ! $self->files_given
                          ? sprintf( "Inserting data for %s. Date [%s]\n",
                                      $name, $date->ymd
                                    )
                          : "Inserting given files\n"
                        );

      if (! $obs[0]) {

        $self->_print_text( "\tNo observations found for instrument $name\n\n" );
        next;
      }

      $tables[1] = $inst->table;

      $columns->{$name} = $self->get_columns( $inst->table, $dbh );
      $columns->{FILES} = $self->get_columns( 'FILES', $dbh );

      # Need to create a hash with keys corresponding to the observation number
      # (an array won't be very efficient since observations can be missing and
      # run numbers can be large). The values in this hash have to be a reference
      # to an array of Info::Obs objects representing each subsystem. We need to
      # construct new Obs objects based on the subsystem number.
      # $observations{$runnr}->[$subsys_number] should be an Info::Obs object.

      for my $obs (@obs) {
        my @subhdrs = $obs->subsystems;
        $observations->{$obs->runnr} = \@subhdrs;
      }

      my $added = $self->insert_observations( 'db' => $db,
                                              'instrument' => $inst,
                                              'columns' => $columns,
                                              'dict'    => \%dict,
                                              'obs' => $observations,
                                            );


      push @files_added, @{ $added }
        if $added && scalar @{ $added };
    }

    return \@files_added;
  }

=item B<insert_observations>

Inserts a row  in "FILES", "COMMON", and instrument related tables for
each observation for each subscan and subsystem used.  Every insert
per observation is done in one transaction.

It takes a hash of database handle; an instrument object
(I<JSA::EnterData::ACSIS> or I<JSA::EnterData::SCUBA2>); hash
reference of observations (run number as keys, array reference of sub
headers as values); a hash reference of columns (see I<get_columns>);
and a hash reference of dictionary (see I<create_dictionary>).

  $enter->insert_observations( 'dbhandle' => $dbh,
                                'instrument' => $inst,
                                'columns' => \%cols,
                                'dict'    => \%dict,
                                'obs'     => \%obs,
                              );

It is called by I<prepare_and_insert> method.

=cut

  sub insert_observations {

    my ( $self, %args ) = @_ ;

    my ( $obs ) = map { $args{ $_ } } qw[ obs ];

    # Pass everything but observations hash reference to other subs.
    my %pass_args =
      map { $_ => $args{ $_ } } qw[ instrument db columns dict ];

    my @success;

    my %run =
      ( 'inserted' =>
          sub {
            my ( $self, %arg ) = @_;

            push @success, map { $_->filename } @{ $arg{'obs'} };
            return;
          },

        'simulation' =>
          sub {
            my ( $self, %arg ) = @_;

            my $xfer  = $self->_get_xfer_unconnected_dbh();
            $xfer->put_simulation( $arg{'file-id'} );
            return;
          },

        'error' =>
          sub {
            my ( $self, %arg ) = @_;

            my $xfer  = $self->_get_xfer_unconnected_dbh();
            $xfer->put_error( $arg{'file-id'} );
            return;
          },

        'nothing-to-do' => sub {},
      );

    my ( @sub_obs, @base );
    RUN:
    for my $runnr (sort {$a <=> $b} keys %{ $obs } ) {

      @sub_obs =  grep { $_ } @{ $obs->{ $runnr } };

      @base = map { $_->simple_filename } @sub_obs;

      my $ans;
      try {
        $ans = $self->insert_obs_set( 'run-obs' => \@sub_obs,
                                      'file-id' => \@base,
                                      %pass_args,
                                    );
      }
      catch JSA::Error with {

        my ( $e ) = @_;

        $ans = 'error';
      };
      next unless defined $ans;

      if ( exists $run{ $ans } ) {

        $run{ $ans }->( $self,
                        'obs'     => \@sub_obs,
                        'file-id' => \@base,
                      );
      }
      else {

        throw JSA::Error::BadArgs "Do not know what to run for state '$ans'."
      }
    }

    return \@success;
  }

  # For each observation:
  # 1. Insert a row in the COMMON table.
  # 2. Insert a row in the [INSTRUMENT] table for each subsystem used.
  # 3. Insert a row in the FILES table for each subscan
  #
  # fails, the entire observation fails to go in to the DB.
  sub insert_obs_set {

    my ( $self, %arg ) = @_;

    my ( $inst, $db, $run_obs, $files ) =
     map { $arg{ $_ } } qw[ instrument db run-obs file-id ];

    my $dbh  = $db->handle();
    my @file = @{ $files };

    my %pass_arg =
      map { $_ => $arg{ $_ } } qw[ instrument columns dict ];

    for ( @file ) {

      if ( exists $touched{ $_ } ) {

        $self->_print_text( "\talready processed: $_\n" )
          if 1 < $self->verbosity;

        return;
      }
    }

    @touched{ @file } = ();

    for my $obs ( @{ $run_obs } ) {

      if ( $inst->can( 'fill_max_subscan' ) ) {

        $inst->fill_max_subscan( $obs->hdrhash, $obs );
      }

      if ( $inst->can( 'transform_header' ) ) {

        my ( $hash , $array ) = $inst->transform_header( $obs->hdrhash );
        $obs->hdrhash( $hash );
      }
    }

    my $common_obs = $run_obs->[0]
      or do {
              $self->_print_text( 'XXX First run obs is undefined|false; nothing to do.' );
              return 'nothing-to-do';
            };

    # Break hash tie by copying & have an explicit anonymous hash ( "\%{ ... }"
    # does not untie).  This is so that a single element array reference when
    # assigned to one of the keys is assigned as reference (not as the element
    # contained with in).
    my $common_hdrs = { %{ $common_obs->hdrhash } };

    $self->_print_text( sprintf "\t[%s]... ", join ', ', @file );

    if ( ! $self->process_simulation && $common_hdrs->{SIMULATE} ) {

      $self->_print_text( "simulation data; skipping\n" );
      return 'simulation';
    }

    # XXX Skip badly needed data verification for scuba2 until implemented.
    unless ( JSA::EnterData::SCUBA2->name_is_scuba2( $inst->name ) ) {

      my $verify = JCMT::DataVerify->new( 'Obs' => $common_obs )
        or do {
                my $log = Log::Log4perl->get_logger( '' );
                $log->logdie( "XXX Could not make JCMT::DataVerify object: $!" );
              };

      my %invalid = $verify->verify_headers;

      for (keys %invalid) {

        my $val = $invalid{$_}->[0];
        if ( $val =~ /does not match/i ) {

          $self->_print_text( "$_ : $val\n" );
          undef $common_hdrs->{$_};
        } elsif ( $val =~ /should not/i ) {

          $self->_print_text( "$_ : $val\n" );
          undef $common_hdrs->{$_} if $common_hdrs->{$_} =~ /^UNDEF/ ;
        }
      }
    }

    #$dbh->begin_work if $self->load_header_db;
    $db->begin_trans() if $self->load_header_db;

    $self->fill_headers_COMMON( $common_hdrs, $common_obs );

    my $error =
      $self->_modify_db_on_obsend( %pass_arg,
                                  'dbhandle' => $dbh,
                                  'table'    => 'COMMON',
                                  'headers'  => $common_hdrs,
                                );

    if ( $dbh->err() ) {

      $db->rollback_trans();
      $self->_print_error_simple_dup( $error );

      return 'nothing-to-do'
        if $self->_is_insert_dup_error( $error );

      return 'error';
    }

    unless ( $self->update_only_obstime() ) {

      $self->add_subsys_obs( %pass_arg,
                              'db'  => $db,
                              'obs' => $run_obs,
                            )
        or return 'error';
    }

   try {
    $db->commit_trans() if $self->load_header_db;

   }
   catch Error::Simple with {

     my ( $e ) = @_;
     throw JSA::Error $e;
   }

    $self->_print_text( "successful\n" );

    return 'inserted';
  }
}

=item B<skip_state_setting>

Returns truth value to indicate if to skip state setting in transfer
table, when no arguments given.  Default is not to skip.

  print "Not setting state" if $enter->skip_state_setting();

If a truth value is given, it is stored for later use.

  $enter->skip_state_setting( 0 );

=cut

sub skip_state_setting {

  my $self = shift @_;

  my $store = 'skip-state';

  return $self->{ $store } unless scalar @_;

  $self->{ $store } = !! $_[0];
  return;
}


=item B<update_only_obstime>

Returns a truth value to inidicate if to update only the times for an
observattion run.

  print "Only date obs & end will be updated"
    if $enter->update_only_obstime();

=cut

sub update_only_obstime {

  my ( $self) = @_;

  return $self->{'obstime-only'};
}

sub _filter_header {

  my ( $self, $inst, $obs, %ignore ) = @_;

  return
    unless scalar @{ $obs };

  return @{ $obs }
    if $inst->name eq 'ACSIS';

  my $remove_ok =
    sub {

      my ( $href, $key ) = @_;

      return
        unless exists $href->{ $key }
        && defined $ignore{ $key };

      my $present = $href->{ $key };
      return
        defined $present
        && any
            { looks_like_number( $_ )
                ? $present == $_
                : $present eq $_
            }
            ( ref $ignore{ $key }
              ? @{ $ignore{ $key } }
              : $ignore{ $key }
            )
            ;

      };

  my @new;
  OBS:
  for my $cur ( @{ $obs } ) {

    my $header = $cur->hdrhash;

    IGNORE:
    for my $key ( keys %ignore ) {

      if ( $remove_ok->( $header, $key ) ) {

        $self->_print_text( sprintf 'Ignoring observation with %s = %s',
                              $key, $header->{ $key }
                          );

        next OBS;
      }

      push @new, $cur;
      my @subhead = $header->{'SUBHEADERS'} ? @{ $header->{'SUBHEADERS'} } : ();

      next OBS
        unless scalar @subhead;

      my @new_sub;
      SUBHEAD:
      for my $sub ( @subhead ) {

        if ( $remove_ok->( $sub, $key ) ) {

          $self->_print_text( sprintf 'Ignoring subheader with %s = %s',
                                $key, $sub->{ $key }
                            );

          next SUBHEAD;
        }

        push @new_sub, $sub;
      }
      $new[-1]->{'SUBHEADERS'} = [ @new_sub ];
    }
  }

  return @new;
}

=item B<_get_obs_group>

When no files are provided, returns a L<OMP::Info::ObsGroup> object
given instrument name and date as a hash.

  $obs = $enter->_get_obs_group( 'name' => 'ACSIS',
                                  'date' => '2009-06-09'
                                );

Else, returns a L<OMP::Info::ObsGroup> object created with already
given files (see I<files> method).

  $obs = $enter->_get_obs_group;

=cut

sub _get_obs_group {

  my ( $self, %args ) = @_;

  my $log = Log::Log4perl->get_logger( '' );

  my $xfer = $self->_get_xfer_unconnected_dbh();

  my %obs = ( 'nocomments' => 1,
              'retainhdr'  => 1,
              'ignorebad'  => 1,
              'header_search' => 'files'
            );

  require OMP::Info::Obs;

  # Prime file list from database if possible.
  #$self->_get_files_from_db();

  unless ( $self->files_given ) {

    %args =
      ( 'date' => $args{'date'} ,
        'instrument' => $args{'name'},
        %obs
      );
  }
  else {

    my $files = $self->files;
    $xfer->add_found( $files );

    my @obs;
    for my $file (  @{ $files } ) {

      unless ( -r $file && -s _ ) {

        $log->warn( "Unreadble or empty file: $file; skipped.\n" );
        next;
      }

      try {

        push @obs, OMP::Info::Obs->readfile( $file , %obs );
      }
      catch OMP::Error::ObsRead with {

        my ( $err ) = @_;

        throw $err
          unless $err->text() =~ m/^Error reading FITS header from file/;
      };
    }

    my @headers;
    for my $ob ( @obs ) {

      push @headers,
        { 'filename' => $ob->{'FILENAME'}->[0],
          'header' => $ob->hdrhash,
        }
    }

    require OMP::FileUtils;

    my $merged = OMP::FileUtils->merge_dupes( @headers );
    @obs = OMP::Info::Obs->hdrs_to_obs( 'retainhdr' => $obs{'retainhdr'},
                                        'fits'      => $merged
                                        );

    %args =  ( 'obs' => [ @obs ] );
  }

  return
    OMP::Info::ObsGroup->new( %args );
}

sub _get_files_from_db {

  my ( $self ) = @_;

  return if $self->path_not_from_db();

  my $xfer = $self->_get_xfer_unconnected_dbh();
  my $tmp  = $xfer->get_found_files();

  # Merge any new & old file in one list.
  $self->files( [ map {  $_ && ref $_ ? @{ $_ } : () } $tmp, $self->files() ] )
    if $tmp && scalar @{ $tmp };

  return;
}

=item B<skip_obs>

Returs a truth value indicating if an observation is a simulation run,
or for which RA/Dec cannot be calculated. It accepts an
L<OMP::Info::Obs> object.  If optional header hash reference (see
L<OMP::Info::Obs/hdrhash>) is not given, it will be retreived from the
given L<OMP::Info::Obs> object.

  $skip = $enter->skip_obs( $inst, $obs );

  $skip = $enter->skip_obs( $inst, $obs, $header );

C<JSA::Error> execption is thrown if header hash (reference) is
undefined.

=cut

sub skip_obs {

  my ( $self, $inst, $obs, $header ) = @_;

  $header = $obs->hdrhash unless defined $header;

  # Alternatively could (silently) return false.
  throw JSA::Error "FITS headers are undefined."
    unless defined $header;

  # Tests are the same which control database changes.
  return
       ( exists $header->{'SIMULATE'} && !! $header->{'SIMULATE'} )
    || ! $self->calc_radec( $inst, $obs, $header );
}

=item B<add_subsys_obs>

Adds subsystem observations, given a hash of database handle; hash
reference of observations (run number as keys, array reference of sub
headers as values); a hash reference of columns (see I<get_columns>);
and a hash reference of dictionary (see I<create_dictionary>).

The observations hash reference is for a given run number, not the
the I<OMP::Info::Objects> in its entirety.

If "load header db" has been set to true (see I<load_header_db>
method), then the database transactions will be rolled back on
a database related error.

Returns true on success, false on failure.

  $ok = $enter->add_subsys_obs( 'dbhandle' => $dbh,
                                'instrument' => $inst,
                                'columns' => \%cols,
                                'dict'    => \%dict,
                                'obs'     => \%obs_per_runnr,
                              );

It is called by I<insert_observations> method.

=cut

sub add_subsys_obs {

  my ( $self, %args ) = @_;

  for my $k ( qw[ instrument db columns dict obs ] ) {

    next
      if exists $args{ $k } && $args{ $k } && ref $args{ $k };

    throw JSA::Error::BadArgs( qq[No suitable value given for ${k}.] );
  }

  my ( $inst, $db, $obs ) = map { $args{ $_ } } qw[ instrument db obs ];

  my $dbh = $db->handle();

  # Need to pass everything but observations to other subs.
  my %pass_args =
    map { $_ => $args{ $_ } } qw[ instrument columns dict ];

  my $subsysnr = 0;
  my $totsub = scalar @{ $obs };

  for my $subsys_obs ( @{ $obs } ) {

    $subsysnr++;
    $self->_print_text( "Processing subsysnr $subsysnr of $totsub\n" );

    # Obtain instrument table values from this Obs object.  Break hash tie.
    my $subsys_hdrs = { %{ $subsys_obs->hdrhash } };

    # Need to calculate the frequency information
    $inst->calc_freq( $self, $subsys_obs, $subsys_hdrs );

    my $grouped;
    if ( $inst->can( 'transform_header' ) ) {

      ( undef, $grouped ) = $inst->transform_header( $subsys_hdrs );
    }

    my $added_files;
    for my $subh ( $grouped ? @{ $grouped } : $subsys_hdrs ) {

      $inst->_fill_headers_obsid_subsys( $subh, $subsys_obs->obsid );

      my $error;
      unless ( $added_files ) {

        $added_files++;
        $self->_change_FILES( 'obs'        => $subsys_obs,
                              'headers'    => $subsys_hdrs,
                              'instrument' => $inst,
                              'db'         => $db,
                              map( { $_ => $pass_args{ $_ } }
                                    qw[ columns dict ],
                                  ),
                            );
      }

      if ( $inst->can( 'merge_by_obsidss' )
           && exists $subsys_hdrs->{'SUBHEADERS'}
          ) {

        my $sys_sub = $subsys_hdrs->{'SUBHEADERS'};
        my @temp = $inst->merge_by_obsidss( $sys_sub );

        @{ $sys_sub } = @{ $temp[0] }
          if scalar @temp;
      }

      $error =
        $self->_modify_db_on_obsend( %pass_args,
                                      'dbhandle' => $dbh,
                                      'table'   => $inst->table,
                                      'headers' => $subh,
                                    );

      if ( $dbh->err() ) {

        $db->rollback_trans() if $self->load_header_db;
        $self->_print_text( "$error\n\n" );
        return 'error';
      }
    }

  }

  return 1;
}

=item B<insert_hash>

Given a table name, a DBI database handle and a hash reference, insert
the hash contents into the table.  Basically a named insert.  Returns
the executed statement output.  (Copied from example in L<DBI>.)

If any of the values in C<%to_insert> are array references multiple
rows will be inserted corresponding to the content.  If more than one
row has an array reference the size of those arrays must be identical.

In case of error, returns the value as returned by C<DBI->execute>.

  $status =
    $enter->insert_hash( 'table'     => $table,
                          'dbhandle' => $dbh,
                          'insert'   => \%to_insert,
                        );

=cut

sub prepare_insert_hash {

  my ( $self, $table, $field_values ) = @_;

  throw JSA::Error "Empty hash reference was given to insert."
    unless scalar keys %{ $field_values };

  my $log = Log::Log4perl->get_logger( '' );

  # Go through the hash and work out whether we have multiple inserts
  my @have_ref;
  my $nrows = 1;
  for my $key (keys %$field_values) {

    my $ref = ref $field_values->{$key}
      or next;

    $log->logdie( "Unsupported reference type in insert hash!\n" )
      unless $ref eq 'ARRAY';

    my $row_count = scalar @{ $field_values->{$key} };
    if (@have_ref) {

      # count rows
    $log->logdie( "Uneven row count in insert hash ARRAY ref for key '$key'",
                  " ($row_count != $nrows) compared to first key '$have_ref[0]'",
                  "(table $table)\n"
                )
        unless $row_count == $nrows;
    } else {

      $nrows = $row_count;
    }
    push(@have_ref, $key);
  }

  # Now create an array of insert hashes with array references unrolled
  my @insert_hashes;
  if (!@have_ref) {

    @insert_hashes = ( $field_values );
  } else {

    # take local copy of the array content so that we do not damage caller hash
    my %local = map { $_ => [ @{$field_values->{$_}} ] } @have_ref;

    # loop over the known number of rows
    for my $i (0..($nrows-1)) {

      my %row = %$field_values;
      for my $refkey (@have_ref) {

        $row{$refkey} = shift @{$local{$refkey}};
      }
      push(@insert_hashes, \%row);
    }
  }

  return \@insert_hashes;
}

sub insert_hash {

  my ( $self, %args ) = @_;

  my ( $table, $dbh, $insert ) =
    @args{ qw[ table dbhandle insert ] };

  return $self->conditional_insert_hash( %args )
    if $self->conditional_insert;

  my $log = Log::Log4perl->get_logger( '' );

  # Get the fields in sorted order (so that we can match with values)
  # and create a template SQL statement. This can be done with the
  # first hash from @insert_hashes

  my @insert_hashes = @{ $insert };

  my @fields = sort keys %{$insert_hashes[0]}; # sort required

  my ( $sql, $sth );

  $sql = sprintf "INSERT INTO %s (%s) VALUES (%s)",
              $table,
              join( "\n, ", @fields),
              join( "\n, ", ('?') x scalar @fields )
              ;

  if (!$self->debug && $self->load_header_db ) {

    $sth = $dbh->prepare($sql)
      or $log->logdie( "Could not prepare sql statement for insert\n", $dbh->errstr, "\n" );
  }

  my ( @prim_key );
  if ( $table eq 'FILES' ) {

    my $prim_key = _get_primary_key( $table );
    @prim_key = ref $prim_key ? @{ $prim_key } : $prim_key ;
  }

  my @file;
  # and insert all the rows
  for my $row (@insert_hashes) {

    my @values = @{$row}{@fields}; # hash slice

    if ($self->debug) {

      $self->_show_insert_sql( $table, \@fields, \@values );
      next;
    }

    next unless $self->load_header_db;

    my $status = $sth->execute(@values);

    if ( $table eq 'FILES' && defined $status && $status > 0
          && ! $self->skip_state_setting()
        ) {

      push @file, $row->{'file_id'};
    }

   return ( $status, scalar @file ? [ @file ] : () )
     if !$status;
  }

  return ( undef, scalar @file ? [ @file ] : () );
}

=item B<conditional_insert_hash>

Given a table name, a DBI database handle and a hash reference, insert
the hash contents into the table.  Basically a named insert.  Returns
a list or number of rows affected.

If any of the values in C<%to_insert> are array references multiple
rows will be inserted corresponding to the content.  If more than one
row has an array reference the size of those arrays must be identical.

  $status =
    $enter->conditional_insert_hash( 'table' => $table,
                                      'dbhandle' => $dbh,
                                      'insert' => \%to_insert,
                                    );

=cut

sub conditional_insert_hash {

  #my ( $self, $table, $dbh, $insert ) = @_;
  my ( $self, %args ) = @_;

  my ( $table, $dbh, $insert ) =
    @args{ qw[ table dbhandle insert ] };

  $dbh->{'syb_show_eed'} = $dbh->{'syb_show_sql'} = 1;

  return $self->insert_hash( %args )
    unless $self->conditional_insert;

  # Get the fields in sorted order (so that we can match with values)
  # and create a template SQL statement. This can be done with the
  # first hash from @insert_hashes

  my @insert_hashes = @{ $insert };

  my @fields = sort keys %{$insert_hashes[0]}; # sort required

  my $prim_key = _get_primary_key( $table );

  # &DBI::prepare is not used for this SQL string as place holders do not work
  # after SELECT clause, need to place the values directly.  See
  # _make_insert_select_sql() && _fill_in_sql() methods.
  my $sql =
    $self->_make_insert_select_sql( 'dbhandle' => $dbh,
                                    'table'    => $table,
                                    'primary'  => $prim_key,
                                    'columns'  => \@fields,
                                  );

  my ( $err, @prim_key, @affected );
  @prim_key = ref $prim_key ? @{ $prim_key } : ( $prim_key ) ;

  my ( $sum, @file );
  for my $row (@insert_hashes) {

    my @values = @{$row}{@fields};

    if ($self->debug) {

      $self->_show_insert_sql( $table, \@fields, \@values );
      next;
    }

    last unless $self->load_header_db;

    my $format =
      join q[ , ],
        $self->_get_cols_format( 'dbhandle' => $dbh,
                                  'table'   => $table,
                                  'columns' => \@fields,
                                  'values'  => \@values,
                                );
    my $tmp = sprintf $sql, $format;
    my $filled =
      sprintf $tmp,
        $self->_quote_vals( 'dbhandle' => $dbh,
                            'table'   => $table,
                            'columns' => \@fields,
                            'values'  => \@values,
                          );

    my @prim_val = map { $row->{ $_ } } @prim_key;

    my $affected = $dbh->do( $filled,
                              $prim_key
                              ? ( undef, @prim_val )
                              : ()
                            );

    if ( $table eq 'FILES' && $affected && $affected > 0
          && ! $self->skip_state_setting()
        ) {

      push @file, $row->{'file_id'};
    }

    return ( $sum, scalar @file ? [ @file ] : () ) unless $affected;

    $sum += $affected;
  }

  return ( $sum, scalar @file ? [ @file ] : () );
}

=item B<_make_insert_select_sql>

Returns a SQL INSERT query string (to be first processed by
C<sprintf>, see I<_fill_in_sql>), given a hash of ...

  table - table name,
  columns - array reference of ordered column names, and
  primary - primary key name.

Purpose of the generated INSERT query string is to avoid adding
duplicate row by checking if the primary key already does not exist.

  $string =
    $self->_make_insert_select_sql( 'table' => $table_name,
                                    'columns' => [ @column_names ],
                                    'primary' => $key,
                                  );


The string returned has a DBI placeholder only for primary key value.
(In DBD::Sybase, possibly within Sybase ASE 15 itself, placeholders
are not allowed after "SELECT" clause.) Something like ...

  INSERT INTO <table>
    SELECT %s, %s, ...
    WHERE NOT EXISTS
      ( SELECT 1 FROM <table> WHERE <primary key> = ? )

=cut

sub _make_insert_select_sql {

  my ( $self, %args ) = @_;

  my ( $dbh, $table, $cols, $primary ) =
    @args{qw[ dbhandle table columns primary ]};

  throw JSA::Error::BadArgs "No primary keys given."
    unless defined $primary;

  my @primary = ref $primary ? @{ $primary } : ( $primary );

  my $where = '';
  for my $k ( @primary ) {

    $where .= ( $where ? ' AND ' : ' ' )
            . qq[ $k = ? ]
            ;
  }

  return
    sprintf q[ INSERT INTO %s (%s) SELECT %%s ]
          . q[ WHERE NOT EXISTS ( SELECT 1 FROM %s WHERE %s ) ],
          $table,
          join( q[ , ], @{ $cols } ),
          $table,
          $where
          ;
}

=item B<_fill_in_sql>

Returns a given format string substitued with given row values (as an
array reference), in addition to a valid database handle, table name,
and column names (as an array reference) in a hash.

  $filled =
    $self->_fill_in_sql( 'sql' => $sql_string,
                          'dbhandle' => $dbh,
                          'table' => $table,
                          'columns' => [ @column_name ]
                          'values' => [ @value ],
                        );

This is a workaround for lack of support of place holders in subquery
(for the values to be SELECT'd; see definition of
I<_make_insert_select_sql> method).

=cut

sub _fill_in_sql {

  my ( $self, %args ) = @_;

  return
    sprintf $args{'sql'}, $self->_quote_vals( %args );
}

{
  my ( %types, %val_format, $num_re );

  sub _init_num_regex {

    $num_re = qr{\b ( decimal | boolean | float | real | int | bit )}xi
      unless $num_re;

    return;
  }

  sub _init_val_format {

    %val_format =
      ( 'int'     => '%d',
        'bit'     => '%d',
        'boolean' => '%d',
        'decimal' => '%0.16f',
        'float'   => '%0.16f',
        'real'    => '%0.16f',
        ''        => '%s',
        undef     => '%s',
        'char'    => '%s',
        'varchar' => '%s',
      )
      unless scalar keys %val_format;

    return;
  }

  sub _get_format {

    my ( $type ) = @_;

    _init_num_regex();
    _init_val_format();

    no warnings 'uninitialized';
    return $val_format{ ( $type =~ $num_re )[0] };
  }

  sub _get_types {

    my ( $self, $dbh, $table ) = @_;

    return $types{ $table }
      if $types{ $table };

    return
      $types{ $table } = $self->get_columns( $table, $dbh )
  }

  sub _get_cols_format {

    my ( $self, %args ) = @_;

    my ( $dbh, $table, $cols, $vals ) =
      @args{qw[ dbhandle table columns values ]};

    my $size = scalar @{ $cols };

    my $types = $self->_get_types( $dbh, $table );

    my @format;
    for ( my $i = 0; $i < $size; $i++ ) {

      push @format,
        _get_format( $types->{ $cols->[ $i ] } );

      if ( $vals && '%s' ne $format[ $i ] ) {

        my $v = $vals->[ $i ];

        $format[ $i ] = '%s'
          if ! defined $v
          || ( $v && $v =~ m/\bNULL\b/i )
          ;
      }
    }
    return @format;
  }

  sub _quote_vals {

    my ( $self, %args ) = @_;

    my ( $dbh, $table, $values ) = @args{qw[ dbhandle table values ]};
    my $size = scalar @{ $values };

    my $types = $self->_get_types( $dbh, $table );

    # Handle number type values (for consumption in Sybase ASE 15.0) outside of
    # &DBI::quote as it puts quotes around such values.
    _init_num_regex();

    my @val;
    for ( my $i = 0; $i < $size; $i++ ) {

      my $val = $values->[ $i ];
      my $type = $types->{ $args{'columns'}->[ $i ] };

      push @val,
        ! defined $val
        ? 'NULL'
        : $type =~ m/$num_re/
          ? $val
          : $dbh->quote( $val, $type )
          ;
    }
    return @val;
  }
}

=item B<update_hash>

Given a table name, a DBI database handle and a hash reference,
retrieve the current data values based on OBSID or OBSID_SUBSYSNR,
decide what has changed and update the values.

  $enter->update_hash( $table, $dbh, \%to_update );

No-op for files table at the present time.

=cut

sub prepare_update_hash {

  my ( $self, $table, $dbh, $field_values ) = @_;

  return if $table eq 'FILES';

  my $log = Log::Log4perl->get_logger( '' );

  # work out which key uniquely identifies the row
  my $unique_key = _get_primary_key( $table );

  unless ( $unique_key ) {

    $log->logdie( "Major problem with table name: '$table'\n" );
  }

  my $unique_val = $field_values->{$unique_key};

  # run a query with this unique value (but on FILES table more than
  # one entry can be returned - we trap that because FILES for the minute
  # should not need updating

  my $sql = 'select * ';

  my ( %start, %end );
  if ( $table eq 'COMMON' ) {

    my %col_date;
    @col_date{ JSA::DB::TableCOMMON::date_columns() } = ();

    my %range = JSA::DB::TableCOMMON::range_columns();
    @start{ keys %range } = ();
    @end{ values %range } = ();

    $sql = 'select '
            . join ', ',
                map
                { ! exists $col_date{ $_ }
                  ? $_
                  : qq[CONVERT( VARCHAR, $_, 23 ) AS $_]
                }
                JSA::DB::TableCOMMON::column_names()
                ;
  }

  $sql .= " from $table where $unique_key = '$unique_val'";

  my $ref = $dbh->selectall_arrayref( $sql, { Columns=>{} })
    or $log->logdie( "Error retrieving existing content using [$sql]: ", $dbh->errstr, "\n" );

  $log->logdie( "Only retrieved partial dataset: ", $dbh->errstr, "\n" )
    if $dbh->err;

  # how many rows
  my $count = scalar @{ $ref };

  throw JSA::Error::DBError
    "Can only update if the row exists previously! Obsid $unique_val missing.\n"
      if $count == 0;

  $log->logdie( "Should not be possible to have more than one row. Got $count\n" )
    if $count > 1;

  my $indb = $ref->[0];

  my %differ;
  my $ymd_start = qr/^\d{4}-\d{2}-\d{2}/;
  my $am_pm_end = qr/\d\d[APM]$/;

  my $obs_date_re = qr{\bDATE.(?:OBS|END)\b}i;

  my $only_obstime =
    $table eq 'COMMON'
    && $self->update_only_obstime();

  for my $key ( sort keys %{$indb} ) {

    next
      # since that will update automatically
      if $key eq 'last_modified'
      || ! exists $field_values->{$key};

    my $new = $field_values->{$key};
    my $old = $indb->{$key};

    next if ! ( defined $old || defined $new );

    my %test =
      ( 'start' => exists $start{ $key },
        'end'   => exists $end{ $key },
        'old'   => $old,
        'new'   => $new,
      );
    my $in_range = any { $test{ $_ } } (qw[ start end ]);

    next
      if $only_obstime
      && $key !~ $obs_date_re
      ;

    # Not defined currently - inserting new value.
    if ( defined $new && ! defined $old ) {

      $differ{$key} = $new;
      next;
    }

    # Defined in DB but undef in new version - not expecting this but assume
    # this means a null.
    if ( ! defined $new && defined $old) {

      $differ{$key} = undef;
      next;
    }

    # Dates.
    if ( $new =~ $ymd_start && ( $old =~ $ymd_start || $old =~ $am_pm_end ) ) {

      if ( $in_range ) {

        $new = _find_extreme_value( %test,
                                    'new>old' => _compare_dates( $new, $old )
                                  );
      }

      if ( $new ne $old ) {

        $differ{ $key } = $new;
      }

      next;
    }

    if (looks_like_number($new)) {

      if ( $in_range ) {

        $new = _find_extreme_value( %test, 'new>old' => $new > $old );

        $differ{ $key } = $new if $new != $old;
      }
      else {

        if ($new =~ /\./) {

          # floating point
          my $diff = abs($old - $new);
          if ($diff > 0.000001) {

            $differ{$key} = $new;
          }
        }
        elsif ( $new != $old ) {

          $differ{$key} = $new;
        }
      }

      next;
    }

    # String.
    if ( $new ne $old ) {

      $differ{ $key } = $new;
    }
  }

  return
    { 'differ' => { %differ },
      'unique_val' => $unique_val,
      'unique_key' => $unique_key,
    };
}

=item B<_get_primary_key>

Returns the primary key for a given table in C<jcmt> database.

  $primary = _get_primary_key( 'ACSIS' );

=cut

sub _get_primary_key {

  my ( $table ) = @_;

  my %keys =
    ( 'ACSIS'  => 'obsid_subsysnr',
      'COMMON' => 'obsid',
      'FILES'  => [qw{ obsid_subsysnr file_id }],
      'SCUBA2' => 'obsid_subsysnr',
      'transfer' => 'file_id',
    );

  return unless exists $keys{ $table };
  return $keys{ $table };
}

sub update_hash {

  my ( $self, $table, $dbh, $change ) = @_;

  return
    if $table eq 'FILES'
    || ! $change;

  my $log = Log::Log4perl->get_logger( '' );

  my %differ = %{ $change->{'differ'} };

  return 1 unless keys %differ;

  my @sorted = sort keys %differ;

  # Now have to do an UPDATE
  my $changes =
    join ', ',
      map
      { join ' = ',
          $_,
          ( !$self->debug && $self->load_header_db
              ? q[ ? ]
              : $differ{$_} # debug version with unquoted values
          );
      }
      @sorted;

  my $sql = sprintf "UPDATE %s SET %s WHERE %s = '%s'",
            $table,
            $changes,
            $change->{'unique_key'},
            $change->{'unique_val'};

  if ( $self->debug ) {

    $self->_print_text( "$sql\n" );
    return 1;
  }

  if ( $self->load_header_db ) {

    my $sth = $dbh->prepare($sql)
      or $log->logdie( "Could not prepare sql statement for UPDATE\n", $dbh->errstr, "\n" );

    my @bind = map { $differ{$_} } @sorted;

    my $status = $sth->execute( @bind );
    throw JSA::Error::DBError 'UPDATE error: ' . $dbh->errstr() . "\n... with { $sql, @bind }"
      if $dbh->err();

    return $status;
  }

  return 1;
}

=item B<transform_value>

Given a table name, column name, and value to be inserted in a table,
alter the value if the database expects the value to be in a different
format than that of the headers.

  $enter->transform_value($table, \%columns, \%values);

=cut

sub transform_value {

  my ( $self, $table, $columns, $values ) = @_;

  # Transform data hash.  Each data type name contains a hash mapping
  # values from the headers to the values the database expects.
  my %transform_data = (
                        bit => {T => 1,
                                F => 0,},
                        int => {T => 1,
                                F => 0,},
                       );

  for my $column (keys %$values) {

    # Store column's current value
    my $val = $values->{$column};
    next unless defined($val);

    if (exists $columns->{$table}{$column}) {

      # Column is defined for this table, get the data type
      my $data_type = $columns->{$table}{$column};

      if ( $data_type eq 'datetime' ) {

        # Temporarily (needs to be handled at the header source) set a
        # zero date (0000-00-00T00:00:00) to undef.
        ( my $non_zero = $val ) =~ tr/0T :-//d;
        unless ( $non_zero ) {

          undef $values->{$column} ;

          $self->_print_text( sprintf "Converted date [%s] to [undef] for column [%s]\n",
                              $val, $column
                            )
            if $self->debug;
        }
      } elsif (exists $transform_data{$data_type}) {

        if (exists $transform_data{$data_type}{$val}) {

          # This value needs to be transformed to the new value
          # defined in the %transform_data hash
          $values->{$column} = $transform_data{$data_type}{$val};

          $self->_print_text( sprintf "Transformed value [%s] to [%s] for column [%s]\n",
                                $val, $values->{$column}, $column
                            )
            if $self->debug;
        }
      } elsif ($column eq 'lststart' or $column eq 'lstend') {

        # Convert LSTSTART and LSTEND to decimal hours
        my $ha = new Astro::Coords::Angle::Hour($val, units => 'sex');
        $values->{$column} = $ha->hours;

        $self->_print_text( sprintf "Converted time [%s] to [%s] for column [%s]\n",
                              $val, $values->{$column}, $column
                          )
          if $self->debug;
      }
    }
  }
  return 1;
}

=item B<fill_headers_COMMON>

Fills in the headers for C<COMMON> database table, given a headers
hash reference and an L<OMP::Info::Obs> object.

  $enter->fill_headers_COMMON( \%header, $obs );

=cut

sub fill_headers_COMMON {

  my ( $self, $header, $obs  ) = @_;

  # Get date of observation
  my $obsdate = $obs->utdate;

  # Create release_date (end of semester + one year)
  # for the general case but for OBSTYPE != SCIENCE or
  # STANDARD=T the release date is immediate
  # Surveys are special since we do not know when the release date
  # should be at ingest time
  my $release_date;

  if ( $obs->projectid =~ /^mjls/i && $obs->isScience) {

    # Survey. Should properly check the SURVEY FITS header
    $release_date = DateTime->new( 'month' => 1,
                                   'year' => 2031,
                                   'day' => 1,
                                   'hour' => 0,
                                   'minute' => 0,
                                   'second' => 0,
                                   'time_zone' => 0
                                 );

  # For time being, set the release date in Jun 2010.
  } elsif ( JSA::EnterData::SCUBA2->name_is_scuba2( $header->{'INSTRUME'} )
        || JSA::EnterData::SCUBA2->name_is_scuba2( $header->{'BACKEND'} )
      ) {

    # Default currently not to release EC data
    if ($obs->projectid =~ /ec/i) {
      $release_date = DateTime->new( 'month' => 1,
                                     'year' => 2031,
                                     'day' => 1,
                                     'hour' => 0,
                                     'minute' => 0,
                                     'second' => 0,
                                     'time_zone' => 0
                                   );
    } elsif ( $obs->isScience ) {
      # Treat this as SRO for now
      $release_date = DateTime->new( 'month' => 5,
                                     'year' => 2010,
                                     'day' => 1,
                                     'hour' => 0,
                                     'minute' => 0,
                                     'second' => 0,
                                     'time_zone' => 0
                                   );

    } else {
      # Release it
      $release_date = OMP::DateTool->yesterday(1);
    }

  }
  elsif ($obs->isScience) {

    # semester release
    my $semester = OMP::DateTool->determine_semester( date => $obsdate,
                                                      tel => 'JCMT'
                                                    );
    my ($sem_begin, $sem_end) =
      OMP::DateTool->semester_boundary( semester => $semester, tel => 'JCMT' );

    # Use DateTime so that we can have proper addition. Add 1 year 1 day because
    # sem_end refers to the UT date and doesn't specify hours/minutes/seconds
    $release_date =
        DateTime->from_epoch( epoch => $sem_end->epoch, time_zone => 'UTC' )
      + DateTime::Duration->new( years => 1, hours => 23, minutes => 59, seconds => 59 );

  } else {

    # immediate release
    $release_date = OMP::DateTool->yesterday(1);
  }
  $header->{'release_date'} = $release_date->strftime($self->sybase_date_format);

  $self->_print_text( sprintf "Created header [release_date] with value [%s]\n",
                        $header->{'release_date'}
                    )
    if $self->debug;

  # Create last_modified
  my $today = gmtime;
  $header->{'last_modified'} = $today->strftime($self->sybase_date_format);

  $self->_print_text( sprintf "Created header [last_modified] with value [%s]\n",
                        $header->{'last_modified'}
                    )
    if $self->debug;

  if ( exists $header->{'INSTRUME'} && ! defined $header->{'BACKEND'} ) {

    $header->{'BACKEND'} = $header->{'INSTRUME'};
  }

  _fix_dates( $header );
  return;
}

# Sybase ASE 15 cannot convert '0.000000000000000e+00' to a datetime value.  Set
# those to undef, thus NULL.
sub _fix_dates {

  my ( $header ) = @_;

  my $date_re = qr{ (?: \b date | dat(?: en | st )\b ) }xi;

  for my $k ( keys %{ $header } ) {

    next unless $k =~ $date_re;

    my $date = $header->{ $k };
    undef $header->{ $k }
      if ! $date
      || ( looks_like_number( $date ) && 0 == $date )
          ;
  }

  return;
}

=item B<fill_headers_FILES>

Fills in the headers for C<FILES> database table, given a
L<JSA::EnterData::ACSIS> or L<JSA::EnterData::SCUBA2> object, a
headers hash reference and an L<OMP::Info::Obs> object.

  $enter->fill_headers_FILES( $inst, \%header, $obs );

=cut

sub fill_headers_FILES {

  my ( $self, $inst, $header, $obs ) = @_;

  my $log = Log::Log4perl->get_logger( '' );

  # Create file_id - also need to extract NSUBSCAN from subheader if we have more
  # than one file. (although simply using a 1-based index would be sufficient)
  my @files = $obs->simple_filename;
  $header->{'file_id'} = \@files;

  # We need to know whether a nsubscan header is even required so %columns really
  # needs to be accessed. For now we kluge it.
  unless ( exists $header->{'nsubscan'} ) {

    if (scalar(@files) > 1) {

      $header->{'nsubscan'} = [ map { $_->value('NSUBSCAN') } $obs->fits->subhdrs ];
    } elsif (exists $header->{'NSUBSCAN'}) {

      # not really needed because the key becomes case insensitive
      $header->{'nsubscan'} = $header->{'NSUBSCAN'};
    } else {

      $log->logdie( "Internal error - NSUBSCAN does not exist yet there is only one file!\n" );
    }
  }

  $self->_print_text( sprintf "Created header [file_id] with value [%s]\n",
                        join ',', @{ $header->{'file_id'} }
                    )
    if $self->debug;

  $inst->_fill_headers_obsid_subsys( $header, $obs->obsid );

  # Further work needs to be done for SCUBA2.
  if ( my $fill = $inst->can( 'fill_headers_FILES' ) ) {

    $inst->$fill( $header, $obs );
  }

  return;
}

=item B<get_columns>

Given a table name and a DBI database handle object, return a hash reference
containing columns with their associated data types.

  $cols = $enter->get_columns( $table, $dbh )

=cut

sub get_columns {

  my ( $self, $table, $dbh ) = @_;

  return {} unless defined $dbh;

  # Do query to retrieve column info (using the sp_columns stored procedure)
  my $col_href = $dbh->selectall_hashref("sp_columns $table", "column_name")
     or throw JSA::Error
            "Could not obtain column information for table [$table]: "
          . $dbh->errstr . "\n";

  my %result;
  for my $col (keys %$col_href) {

    $result{$col} = $col_href->{$col}{type_name};
  }

  return \%result;
}

=item B<get_insert_values>

Given a hash of a table name; a hash reference containing table column
information (see global hash %columns); a hash reference containing
the dictionary contents; and a hash reference containing observation
headers, return a hash reference with the table's columns as the keys,
and the insertion values as the values.

For FILES table, an additional hash reference is needed to list the
already processed files.  Keys are the (base) file names, values could
be anything.

  $vals =
    $enter->get_insert_values( 'table' => $table,
                                'columns' => \%columns,
                                'dict' => \%dictionary,
                                'headers' => \%hdrhash,
                              );

=cut

sub get_insert_values {

  my ( $self, %args ) = @_;
  #my ( $self, $table, $columns, $dictionary, $hdrhash ) = @_;

  my ( $table, $columns ) = map { $args{ $_ } } qw[ table columns ];

  for ( qw[ SCUBA-2 ] ) {

    $columns->{ $table } = $columns->{ $_ }
      if 'scuba2' eq lc $table
      && ! exists $columns->{ $table }
      && exists $columns->{ $_ }
      ;
  }

  # Map headers to columns, translating from the dictionary as
  # necessary.

  my $main = $self->extract_column_headers( %args );

  # Do value transformation
  $self->transform_value($table, $columns, $main);

  return $main;
}

sub extract_column_headers {

  my ( $self, %args ) = @_;

  my ( $hdrhash, $table, $columns, $dict ) =
    map { $args{ $_ } } qw[ headers table columns dict ];

  my %values;

  for my $header (sort { lc $a cmp lc $b } keys %$hdrhash) {

    my $alt_head = lc $header;

    if (exists $columns->{$table}{ $alt_head }) {

      $values{ $alt_head } = $hdrhash->{$header};
    }
    elsif ( exists $dict->{ $alt_head }
            && exists $columns->{ $table }{ $dict->{ $alt_head } } ) {

      # Found header alias in dictionary and column exists in table
      my $alias = $dict->{ $alt_head };
      $values{$alias} = $hdrhash->{$header};

      $self->_print_text( "Mapped header [$header] to column [$alias]\n" )
        if $self->debug;
    }

    $self->_print_text( "Could not find alias for header [$header].  Skipped.\n" )
      if $self->debug and ! exists $values{ $alt_head };
  }

  return \%values;
}

=item B<get_max_idkey>

Given the COMMON table name and a database handle object, return the
highest idkey/index in the COMMON table.

  $idkey = $enter->get_max_idkey( $common_table, $dbh );

=cut

sub get_max_idkey {

  my ( $self, $table, $dbh ) = @_;

  return 1 unless $self->load_header_db;

  my $log = Log::Log4perl->get_logger( '' );

  my $sth = $dbh->prepare_cached("select max(idkey) from $table");
  $sth->execute
    or $log->logdie( "Could not obtain max idkey: ", $dbh->errstr, "\n" );

  my $result = $sth->fetchall_arrayref;
  my $max = $result->[0]->[0];

  return $max;
}

=item B<create_dictionary>

Given the location of the data dictionary, return a hash containing
the dictionary contents.

  %dictionary = $enter->create_dictionary( $dictionary );

=cut

sub create_dictionary {

  my ( $self ) = @_;

  my $dictionary = $self->{'dict'};
  my %dict;

  my $log = Log::Log4perl->get_logger( '' );

  open my $DICT, '<', $dictionary
    or $log->logdie( "Could not open data dictionary '$dictionary': $!\n" );

  my @defs = grep { $_ !~ /^\s*(?:#|$)/ } <$DICT>;  # Slurp!

  close $DICT
    or $log->logdie( "Error closing data dictionary '$dictionary': $!\n" );

  for my $def (@defs) {

    $def =~ s/\s+$//;
    if ( $def =~ /(.*?)\:\s(.*)/ ) {

      # Store each dictionary alias as a key whose value is a column name
      map { $dict{$_} = "$1" } split /\s/, "$2";
    }
  }
  return %dict;
}

=item B<calc_radec>

Calculate RA/Dec extent (ICRS) of the observation and the base
position.  It populates header with corners of grid (in decimal
degrees).  Status is perl status: 1 is good, 0 bad.

  $status = JSA::EnterData->calc_radec( $inst, $obs, $header );

=cut

sub calc_radec {

  my ( $self, $inst, $obs, $headerref ) = @_;

  my $log = Log::Log4perl->get_logger( '' );

  # Filenames for a subsystem
  my @filenames = $obs->filename;

  # Now need to write these files to  temp file
  my $fh = File::Temp->new;
  print $fh map { $_ ."\n" } @filenames;
  close($fh);

  # PA (may not be present)
  my $pa = $headerref->{MAP_PA};
  $pa *= -1 if defined $pa;

  my @command = $inst->get_bound_check_command( $fh, $pa );
  my ( $systat );
  try {

    ( undef, undef, $systat ) =
      run_star_command( $command[0], @command[ 1 .. $#command ] );
  }
  catch JSA::Error::StarlinkCommand with {

    my ( $err ) = @_;
    $log->error( $err->text );
    printf "Bound calculation error with files starting with %s; see log\n",
      $filenames[0];
  };
  # Allow JSA::Error::BadExec error to move up.

  # run_star_command() throws Error when $systat != 0.
  return if ! defined $systat || $systat != 0;

  # Get the bounds
  my %result =
    ( 'REFLAT' => undef,
      'REFLON' => undef,
    );

  require File::Basename;
  my $prog = File::Basename::fileparse( $command[0], '' );

  for my $k ( qw/ FTL FBR FTR FBL /) {

    my $res = qx{ /star/bin/kappa/parget $k $prog };
    $res =~ s/^\s+//;
    $res =~ s/\s+$//;
    $result{$k} = [ map {Astro::Coords::Angle->new( $_, units => 'rad') } split(/\s+/,$res) ];
  }

  for my $corner (qw/ tl br tr bl / ) {

    my $parkey = "F".uc($corner);
    $headerref->{"obsra$corner"} = $result{$parkey}->[0]->degrees;
    $headerref->{"obsdec$corner"} = $result{$parkey}->[1]->degrees;
  }

  # and the base position (easier to just ask SMURF rather than opening the file) but
  # for a planet or comet/asteroid this will not be correct and should be set to undef
  # This means we have to look at JCMTSTATE anyway (but we still ask SMURF because that
  # will save us doing coordinate conversion)

  my $tracksys =
    $self->_find_header( 'headers' => $headerref,
                          'name' => 'TRACKSYS',
                          'value' => 1,
                          'test' => 'true',
                        );

  my %state;
  unless ( $tracksys ) {

    ( undef, %state ) = $self->read_ndf( $filenames[0], qw/ TCS_TR_SYS / );
    $log->logdie( "Error reading state information from file $filenames[0]\n" )
      unless keys %state;
  }

  my $not_app_azel =
    sub {
      return
        defined $_[0]
        && length $_[0]
        && $_[0] !~ m/^(?:APP|AZEL)/i
    };

  # check for APP or AZEL (should never be AZEL!)
  if ( $not_app_azel->( $tracksys )
        || ( exists $state{TCS_TR_SYS} && $not_app_azel->( $state{TCS_TR_SYS} ) )
      ) {

    for my $k (qw/ REFLON REFLAT / ) {

      my $res = qx{ /star/bin/kappa/parget $k $prog };
      chomp($res);
      $result{$k} = $res;
    }

    # convert to radians
    $result{REFLON} = Astro::Coords::Angle::Hour->new( $result{REFLON}, units => 'sex', range => '2PI' )->degrees;
    $result{REFLAT} = Astro::Coords::Angle->new( $result{REFLAT}, units => 'sex', range => 'PI' )->degrees;

  }

  $headerref->{obsra} = $result{REFLON};
  $headerref->{obsdec} = $result{REFLAT};

  return 1;
}

=item B<read_ndf>

Open an NDF file, read the frameset and the first entry from the
supplied list of JCMTSTATE components (can be empty).

Returns hash of JCMTSTATE information and the Starlink::AST object.

  ($wcs, %state) = JSA::EnterData->read_ndf( $file, @state );

returns empty list on error.  In scalar context just returns WCS
frameset...

  $wcs = JSA::EnterData->read_ndf( $file );

On error, flushes error to standard error and returns empty list.

=cut

sub read_ndf {

  my ( $self, $file, @statekeys ) = @_;

  my $wcs;
  my $E;
  try {
    $wcs = read_wcs( $file );
  } catch JSA::Error::FatalError with {
    $E = shift;
  } otherwise {
    $E = shift;
  };

  if (defined $E) {
    print STDERR $E;
    return ();
  }

  # if we have keys to read and are in list
  # context, read the state
  my %state;
  if (@statekeys && wantarray() ) {

    try {
      %state = read_jcmtstate( $file, 'start', @statekeys );
    } catch JSA::Error::FatalError with {
      $E = shift;
    } otherwise {
      $E = shift;
    };
    if (defined $E) {
      print STDERR $E;
      return ();
    }
  }

  return wantarray ? ( $wcs, %state ) : $wcs;
}

sub _change_FILES {

  my ( $self, %arg ) = @_;

  my $table = 'FILES';

  my ( $headers, $obs, $db, $inst ) =
    @arg{qw[ headers obs db instrument ]};

  my $dbh = $db->handle();

  my $old_cond = $self->conditional_insert;
  $self->conditional_insert( 1 )
    if $self->update_mode;

  # Create headers that don't exist
  $self->fill_headers_FILES( $inst, $headers, $obs );

  my $insert_ref =
    $self->get_insert_values( 'table'    => $table,
                               'headers' => $headers,
                              map( { $_ => $arg{ $_} }
                                    qw[ columns dict ]
                                  ),
                            );

  my ( $files , $error );
  try {

    _verify_file_name( $insert_ref->{'file_id'} );

    my $hash = $self->prepare_insert_hash( $table, $insert_ref );

    ( $error, $files ) =
      $self->insert_hash( 'table'    => $table,
                          'dbhandle' => $dbh,
                          'insert' => $hash,
                        );

    $error = $dbh->errstr
      if $dbh->err();
  }
  catch JSA::Error with {

    $error = shift @_;
  };

  $self->conditional_insert( $old_cond );

  if ( $dbh->err() ) {

    $db->rollback_trans() if $self->load_header_db();
    $self->_print_error_simple_dup( $error );
    return;
  }

  if ( $files && scalar @{ $files } ) {

    my $xfer = $self->_get_xfer_unconnected_dbh();
    $xfer->put_ingested( $files );
  }

  return;
}

=item B<_show_insert_sql>

Prints insert SQL statement, given table name, column names, and
column values.

  $enter->_show_insert_sql( 'FILES', \@names, \@values );

=cut

sub _show_insert_sql {

  my ( $self, $table, $fields, $values ) = @_;

  my @val = @{ $values };

  # print out some SQL that is not going to be executed
  for ( @val ) {

    unless ( defined $_ ) {

      $_ = 'NULL';
    } else {

      $_ = "\'$_\'"
        if /([a-zA-Z]|\s+)/ and $_ !~ /e\+/ ;
    }
  }

  $self->_print_text( sprintf "-----> SQL: INSERT INTO %s (%s) VALUES (%s)\n",
                        $table,
                        join( ', ', @{ $fields } ),
                        join( ', ', @val )
                    );

  return;
}

=item B<_update_or_insert>

It is a wrapper around I<update_hash> and I<insert_hash> methods.  It
calls I<update_hash> if value returned by I<update> method is true;
else, I<insert_hash> method is called.

Returns the error string the database handle, given a hash with
C<table>, C<columns>, C<dict>, C<headers> as keys.  For details about
values, see I<insert_hash>, I<update_hash>, and I<get_insert_values>
methods.

  $enter->_update_or_insert( %hash );

=cut

sub _update_or_insert {

  my ( $self, %args ) = @_;

  my $vals = $self->get_insert_values( %args );

  my $table = $args{'table'};

  my $ok;
  if ( $self->update_mode ) {

    my $change = $self->prepare_update_hash( @args{qw/ table dbhandle /}, $vals );
    $ok = $self->update_hash( @args{qw/ table dbhandle /}, $change );
    $ok = defined $ok;
  }
  else {

    $ok =
      $self->
        _combined_prepare_insert_hash( $vals,
                                  map { $_ => $args{ $_ } } qw[ table dbhandle ]
                                );
  }

  return $args{'dbhandle'}->errstr;
}

# KLUDGE to avoid duplicate inserts due to same obsid.  First hash reference
# most likely have undef (AZ|AM|EL)(START|END).
sub _apply_kludge_for_COMMON {

  my ( $self, $vals ) = @_;

  return
    unless ref $vals eq 'ARRAY'
    || 1 < scalar @{ $vals };

  my %val;
  for my $v ( @{ $vals } ) {

    # Last one "wins".
    $val{ $v->{'obsid'} } = $v;
  }

  return [ map { $val{ $_ } } keys %val ];
}

sub _modify_db_on_obsend {

  my ( $self, %args ) = @_;

  # (Try to) Obey update_mode() as usual.
  unless ( $self->_find_header( 'headers' => $args{'headers'},
                                'name' => 'OBSEND',
                                'test' => 'true'
                              )
          ) {

    my ( $err_text, $try_insert );
    try {

      $err_text = $self->_update_or_insert( %args );
    }
    catch JSA::Error::DBError with {

      my ( $err ) = @_;

      # Swallow case of zero rows affected.
      throw JSA::Error::DBError $err
        unless $err->text =~/Can.+update if the row exists previously/i;

      $try_insert++;
    };

    return $err_text unless $try_insert;

    return
      $self->
      _combined_prepare_insert_hash( $self->get_insert_values( %args ),
                                map { $_ => $args{ $_ } } qw[ table dbhandle ]
                              );
  }

  my $old_insert = $self->conditional_insert;

  my $old_mode = $self->update_mode;
  $self->update_mode( 1 );

  my $table = $args{'table'};
  my $vals = $self->get_insert_values( %args );
  my $val_count;
  {
    my $key = (keys %{ $vals } )[0];
    $val_count = ref $vals->{ $key } ? scalar @{ $vals->{ $key } } : 1;
  }
  my $affected;

  # Try UPDATE.
  unless ( $table eq 'FILES' ) {

    my $change;
    try {
      $change =
        $self->prepare_update_hash( @args{qw/ table dbhandle /}, $vals );
    }
    catch JSA::Error::DBError with {

      my ( $err ) = @_;

      # Swallow case of zero rows affected.
      throw JSA::Error::DBError $err
        unless $err->text =~/Can.+update if the row exists previously/i;
    };

    $affected =
      $self->update_hash( @args{qw/ table dbhandle /}, $change );
  }

  if ( ! $affected || $val_count > $affected ) {

    $self->update_mode( 0 );

    # Use conditional insert so that on INSERT failure $dbh->{'AutoCommit'} is
    # NOT set to 1, which breaks the existing transaction setup elsewhere .
    $self->conditional_insert( 1 );

    $self->
      _combined_prepare_insert_hash( $vals,
                                map { $_ => $args{ $_ } } qw[ table dbhandle ]
                              );
  }

  $self->update_mode( $old_mode );
  $self->conditional_insert( $old_insert );

  return $args{'dbhandle'}->errstr;
}

sub _combined_prepare_insert_hash {

  my ( $self, $vals, %args ) = @_;

  my $table = $args{'table'};

  $vals = $self->prepare_insert_hash( $table, $vals );

  $vals = $self->_apply_kludge_for_COMMON( $vals )
    if 'COMMON' eq $table ;

  return $self->insert_hash( %args,
                              'insert' => $vals
                            );
}

=item B<_find_header>

Returns a list of header values or a truth value, given a hash with
I<headers> and I<name> as the required keys. Respective hash values
are a header hash reference and header name to search for.  Default
behaviour is to return a truth value if the given header exists.
Returns nothing if the header is missing or specified test fails.
C<SUBEHEADERS> are also searched along with the main header hash.

  print 'OBSEND header exists'
    if $enter->_find_header( 'headers' => $hdrhash,
                             'name' => 'OBSEND',
                             'test' => 'exists'
                            );

Test for the header value being true or defined can be specified by
providing I<test> key with value of "true" or "defined".

Instead of receiving a truth value, actual header values can be
obtained by specifying I<value> key (associated with any value).

  use Data::Dumper;
  print "Defined OBSEND header value if present: ",
    Dumper( $enter->_find_header( 'headers' => $hdrhash,
                                  'name' => 'OBSEND',
                                  'test' => 'defined',
                                  'value' => undef
                                )
          );

=cut

sub _find_header {

  my ( $self, %args ) = @_;

  my ( $head, $name ) = @args{qw[ headers name ]};

  my $test =
    sub {
      my ( $head, $key ) = @_;

      return unless exists $head->{ $key };
      for ( $args{'test'} ) {

        last unless defined $args{'test'};

        $_ eq 'true' and return !! $head->{ $key };
        $_ eq 'defined' and return defined $head->{ $key };
      }
      return 1;
    };

  my $array = ref $head eq 'ARRAY';

  for my $h ( $array
              ? @{ $head }
              : $head
             ) {

    my $val = $test->( $h, $name ) ? $h->{ $name } : undef;
    if ( $val  ) {

      return
        exists $args{'value'} ? $val : 1 ;
    }
  }

  # Only one level of indirection is checked, i.e. header inside "SUBHEADER"
  # pseudo header with array reference of hash references as value.
  return if $array;

  my $subh = 'SUBHEADERS';
  return $self->_find_header( %args, 'headers' => $head->{ $subh } )
    if exists $head->{ $subh };

  return;
}

=item B<_is_insert_dup_error>

Returns a truth value to indicate if the error was due to insertion of duplicate
row, given a plain string or an L<Error> object.  It compares the expected
Sybase error text.

  $dbh->rollback
    if $enter->_is_insert_dup_error( $dbh->errstr );

=cut

sub _is_insert_dup_error {

  my ( $self, $err ) = @_;

  my $text = ref $err ? $err->text : $err;

  return
    $text
    && $text =~ /insert duplicate key row/i ;
}

=item B<_is_dup_ignored>

Returns a truth value to indicate if the error message was due ignoring the
duplicate key, given a plain string or an L<Error> object.  It compares the
expected Sybase error text.

    $dup_ignore = $enter->_is_dup_ignored( $dbh->errstr );

=cut

sub _is_dup_ignored {

  my ( $self, $err ) = @_;

  my $text = ref $err ? $err->text : $err;

  return
    $text
    && $text =~ /Duplicate key.+ignored/i ;
}

=item <_print_error_simple_dup>

Given a error string, prints the it.  If the string matches Sybase duplicate
insert error message, then prints "File metadata already present".

  $self->_print_error_simple_dup( $dbh->errstr );

=cut

sub _print_error_simple_dup {

  my ( $self, $err ) = @_;

  return
    $self->
    _print_text( $self->_is_insert_dup_error( $err )
                  ? qq[File metadata already present\n]
                  : ref $err
                    ? $err->text . qq[\n\n]
                    : qq[$err\n\n]
                );
}

=item B<_verify_dict>

Verifies that the data dictionary (set via I<new> method) is a
readable file.  On errors, throws L<JSA::Error::FatalError>
exceptions, else returns true.

  $ok = $enter->_verify_dict;

=cut

sub _verify_dict {

  my ( $self ) = @_;

  my $dict = $self->get_dict;

  throw JSA::Error::FatalError( 'No valid data dictionary given' )
    unless defined $dict ;

  throw JSA::Error::FatalError( "Data dictionary, $dict, is not a readable file." )
    unless -f $dict && -r _;

  return 1;
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

  my ( $name ) = @_;

  return unless defined $name;

  my @bad;
  for my $n ( ref $name ? @{ $name } : $name ) {

    push @bad, $n unless looks_like_rawfile( $n );
  }

  my $size = scalar @bad;

  return unless $size;

  throw JSA::Error sprintf "Bad file name%s: %s\n",
                    ( $size > 1 ? 's' : '' ), join ', ' , @bad ;
}

=item B<_print_text>

Prints a given string on currently selected file handle (standard
output normally) if I<verbosity> attribute is true.

=cut

sub _print_text {

  my ( $self, $text ) = @_;

  return
    unless $self->verbosity
        && defined $text;

  my $log = Log::Log4perl->get_logger( '' );
  $log->info( $text );

  return;
}

# JSA::DB::TableTransfer object, to be created as needed.
{
  my %xfer;

  sub _get_xfer {

    my ( $self, $dbh, $name ) = @_;

    $name ||= 'default-xfer';

    return $xfer{ $name }
      if exists  $xfer{ $name }
      && defined $xfer{ $name };

    return
      $xfer{ $name } =
        JSA::DB::TableTransfer->new(  'dbhandle'     => $dbh,
                                      'transactions' => 0,
                                    );
  }
}
=item B<_get_xfer_unconnected_dbh>

It is similar to above I<_get_xfer> method about what it accepts and
returns.  Difference is that this method uses a new database handle
unconnected to the one used elsewhere.

=cut

sub _get_xfer_unconnected_dbh {

  my ( $self, $name ) = @_;

  $name ||= 'xfer-new-dbh';

  require JSA::DB;
  my $db = JSA::DB->new( 'name' => $name );
  $db->use_transaction( 0 );

  return $self->_get_xfer( $db->dbhandle(), $name );
}


sub _compare_dates {

  my ( $new, $old ) = @_;

  # Sometimes a date-time value only has date, in which case time is appended
  # without a 'T'.
  $new =~ s/ /T/;

  $new = make_datetime( $new );
  $old = make_datetime( $old );

  return $new > $old;
}


sub _find_extreme_value {

  my ( %arg ) = @_;

  my $gt = $arg{ 'new>old' };
  my ( $old, $new, $start, $end ) = @arg{qw[ old new start end ]};

  # Smaller|earlier value.
  if ( $start ) {

    return ! $gt ? $new : $old ;
  }

  # Larger|later value.
  if ( $end ) {

    return $gt ? $new : $old;
  }

  throw JSA::Error "Neither 'start' nor 'end' type was specified";
}


1;

=pod

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

Copyright (C) 2006-2008, Science and Technology Facilities Council.
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

