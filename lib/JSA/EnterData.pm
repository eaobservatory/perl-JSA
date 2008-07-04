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
  $enter->upload_mode( 0 );

  $enter->prepare_and_insert;

  # Fill file information.
  $enter->upload_mode( 1 );

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

BEGIN {

  $ENV{'SYBASE'} = '/local/progs/sybase'
    unless exists $ENV{'SYBASE'} && length $ENV{'SYBASE'};

  use constant OMPLIB =>
    '/jac_sw/omp/msbserver'
    #'/Users/timj/work/omp/msbserver'
    #'/home/agarwal/src/scicom/trunk/omp/perl'
    ;

  $ENV{'OMP_CFG_DIR'} = OMPLIB . '/cfg';

  $ENV{'OMP_SITE_CONFIG'} =
    '/home/jcmtarch/enterdata-cfg/enterdata.cfg'
    #'/Users/timj/enterdata.cfg'
    #'/home/agarwal/src/scicom/trunk/archiving/jcmt/cfg/enterdata.cfg'
    ;
}

use strict;
use warnings;
use FindBin;

use lib OMPLIB;
use lib
  "$FindBin::RealBin/../perlmods/JCMT-DataVerify/lib"
  #'/home/agarwal/src/scicom/trunk/archiving/perlmods/JCMT-DataVerify/lib'
  ;

use Data::Dumper;
use File::Temp;
use List::Util qw[ first ];
use Scalar::Util qw[ looks_like_number ];

use Astro::Coords::Angle::Hour;

use JSA::Error qw[ :try ];
use JCMT::DataVerify;

use OMP::DBbackend::Archive;
use OMP::Info::ObsGroup;
use OMP::General;

use DateTime;
use Time::Piece;
use Time::Seconds;

use NDF;

$| = 1; # Make unbuffered

BEGIN {

  # Make sure that bad status from SMURF triggers bad exit status
  $ENV{ADAM_EXIT} = 1;

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

      'instruments' => [ qw[ ACSIS ] ],

      # Location of data dictionary
      # NOTE: This should go in to the OMP config system at some point
      'dict' =>  $FindBin::RealBin . '/import/data.dictionary',

      # Debugging.  Set to true to turn on.  Debugging means interact
      # with the database, but don't actually do any inserts, and be
      # very verbose.
      'debug' => 0,
    );

  #  Generate some accessor functions.
  for my $k ( keys %default ) {

    next
      # Dictionary must be given in constructor.
      if $k eq 'dict'
      # Special handling when date to set is given.
      or $k eq 'date'
      # Validate instruments before setting.
      or $k eq 'instruments'
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

    return $obj;
  }


=item B<instruments>

Returns a list of instruments when no arguments given.  Else, the list
of given instruments is verified and accepted for further use.

  # Currently set.
  $instruments = $enter->instruments;

  # Set ACSIS as the only instrument.
  $enter->instruments( 'ACSIS' );

=cut

  # It is here to have access to %default, in particular
  # $default{'instruments'}.
  sub instruments {

    my $self = shift;

    unless ( scalar @_ ) {

      my $inst = $self->{'instruments'};
      return
        defined $inst && ref $inst ? @{ $inst } : () ;
    }

    for my $inst ( @_ ) {

      throw JSA::Error "Instrument '$inst' is unknown."
        unless first { $inst eq $_ } @{ $default{'instruments'} };
    }

    $self->{'instruments'} = [ @_ ];

    return;
  }

}

=item B<debug>

Returns the set truth value if no arguments given.

  $debug = $enter->debug;

Else, sets the value to turn on or off debugging; returns nothing.

  $enter->debug( 0 );

=item B<date>

Returns the set date if no arguments given.

  $date = $enter->date;

Else, sets the date to date given in C<yyyymmdd> format; returns
nothing.  If date does not match the format, then current date in
local timezone is used.

  $enter->date( '20251013' );

=cut

sub date {

  my $self = shift;

  return $self->{'date'} unless scalar @_;

  my $date = shift || localtime;

  $self->{'date'} =
    ref $date && $date->isa( 'Time::Piece' )
      ? $date
      : Time::Piece->strptime( $date, '%Y%m%d' ) ;

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

  return $self->{'force-db'} unless scalar @_;

  my ( $force ) = @_;

  $self->{'force-db'} = !! $force;

  $self->force_disk( 0 ) if $self->{'force-db'};

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

  return $OMP::ArchiveDB::SkipDBLookup unless scalar @_;

  my ( $force ) = @_;

  # Force observation queries to query files on disk rather than the database.
  $OMP::ArchiveDB::SkipDBLookup = !! $force;

  $self->force_db( 0 ) if $self->{'force-disk'};

  return;
}

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

=cut

=item B<get_dict>

Returns the file name for the data dictionary.

  $dict_file = $enter->get_dict;

=cut

sub get_dict {

  my ( $self ) = @_;
  return $self->{'dict'};
}

=item B<prepare_and_insert>

Inserts observation in database retrieved from disk (see also
I<insert_obs> method) for a date (see also I<date> method).

Date can be given to the method, or can be set via I<new()> or
I<date()> method.  Current date is used if no date has been explicitly
set.

  # Insert either for the set or current date.
  $enter->prepare_and_insert;

  # Insert for Jun 25, 2008.
  $enter->prepare_and_insert( '20080625' );

=cut

sub prepare_and_insert {

  my ( $self, $date ) = @_;

  # Format date first before getting it back.
  $self->date( $date ) if defined $date ;
  $date = $self->date;

  # Tables of interest.  All instruments reference the COMMON table, so it is
  # first on the array.  Actual instrument table will be the second element.
  my @tables = qw/COMMON/;

  my $db = OMP::DBbackend::Archive->new;
  my $dbh = $db->handle;

  # The %columns hash will contain a key for each table, each key's value
  # being an anonymous hash containing the column information.  Store this
  # information for the COMMON table initially.
  my $columns;
  $columns->{$tables[0]} = $self->get_columns( $tables[0], $dbh );

  my %dict = $self->create_dictionary;
  my $observations;

  for my $inst ( $self->instruments ) {

    print "Inserting data for [$inst] Date [". $date->ymd ."]\n";

    $tables[1] = $inst;

    $columns->{$inst} = $self->get_columns( $inst, $dbh );
    $columns->{FILES} = $self->get_columns( 'FILES', $dbh );

    # Retrieve observations from disk.  An Info::Obs object will be returned for
    # each subscan in the observation.  No need to retrieve associated obslog
    # comments. That's <no. of subsystems used> * <no. of subscans objects
    # returned per observation>.
    my $grp = OMP::Info::ObsGroup->new( instrument => $inst,
                                        date => $date,
                                        nocomments => 1,
                                        retainhdr => 1,
                                        ignorebad => 1,
                                      );

    my @obs = $grp->obs;

    if (! $obs[0]) {
      print "\tNo observations found for instrument $inst\n\n";
      next;
    }

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

    $self->insert_obs( $dbh, $observations, $columns, \%dict );
  }

  return;
}

=item B<insert_obs>

Inserts a row  in "FILES", "COMMON", and instrument related tables for
each observation for each subscan and subsystem used.  Every insert
per observation is done in one transaction.

It takes a database handle; hash reference of observations (run number
as keys, array reference of sub headers as values); a hash reference
of columns (see I<get_columns>); and a hash reference of dictionary
(see I<create_dictionary>).

  $enter->insert_obs( $dbh, \%obs, \%cols, \%dict );

It is called by I<prepare_and_insert> method.

=cut

sub insert_obs {

 my ( $self, $dbh, $obs, $cols, $dict ) = @_ ;

  # For each observation:
  # 1. Insert a row in the COMMON table.
  # 2. Insert a row in the [INSTRUMENT] table for each subsystem used.
  # 3. Insert a row in the FILES table for each subscan
  #
  # fails, the entire observation fails to go in to the DB.

  OBS: for my $runnr (sort {$a <=> $b} keys %{ $obs } ) {

    my $common_obs = $obs->{$runnr}->[0];

    # Break hash tie by copying & have an explicit anonymous hash ( "\%{ ... }"
    # does not untie).  This is so that a single element array reference when
    # assigned to one of the keys is assigned as reference (not as the element
    # contained with in).
    my $common_hdrs = { %{ $common_obs->hdrhash } };

    printf "\t[%s]... ", join ',', $common_obs->simple_filename;

    if (($common_hdrs->{SIMULATE})) {

      print "simulation data. Skipping\n";
      next;
    }

    my $verify = JCMT::DataVerify->new( 'Obs' => $common_obs );
    my %invalid = $verify->verify_headers;

    for (keys %invalid) {

      my $val = $invalid{$_}->[0];
      if ( $val =~ /does not match/i ) {

        print "$_ : $val\n";
        undef $common_hdrs->{$_};
      } elsif ( $val =~ /should not/i ) {

        print "$_ : $val\n";
        undef $common_hdrs->{$_} if $common_hdrs->{$_} =~ /^UNDEF/ ;
      }
    }

    # Calculate RA/Dec (ICRS) extent and base position of observation.  Both
    # subsystems are identical so we only have to do this with the first one
    my $cstat = $self->calc_radec( $common_obs, $common_hdrs );
    next unless $cstat;

    $dbh->begin_work if $self->load_header_db;

    $self->fill_headers_COMMON( $common_hdrs, $common_obs );

    my $error =
      $self->_update_or_insert( 'dbhandle' => $dbh,
                                'table'   => 'COMMON',
                                'headers' => $common_hdrs,
                                'columns' => $cols,
                                'dict'    => $dict,
                              );

    if ($error) {

      $dbh->rollback;
      if ($error =~ /insert duplicate key row/i ) {

        print "File metadata already present\n";
      } else {

        print "$error\n\n";
      }
      next;
    }

    $self->add_subsys_obs( $dbh, $obs->{$runnr}, $cols, $dict )
      or next ;

    $dbh->commit if $self->load_header_db;

    print "successful\n";
  }

  return;
}

=item B<add_subsys_obs>

Adds subsystem observations, given a database handle; hash reference
of observations (run number as keys, array reference of sub headers as
values); a hash reference of columns (see I<get_columns>); and a hash
reference of dictionary (see I<create_dictionary>).

If "load header db" has been set to true (see I<load_header_db>
method), then the database transactions will be rolled back on
a database related error.

Returns true on success, false on failure.

  $ok = $enter->add_subsys_obs( $dbh, \%obs, \%cols, \%dict );

It is called by I<insert_obs> method.

=cut

sub add_subsys_obs {

  my ( $self, $dbh, $obs, $cols, $dict ) = @_;

  my $subsysnr = 0;
  my $totsub = scalar @{ $obs };

  for my $subsys_obs ( @{ $obs } ) {

    $subsysnr++;
    print "Processing subsysnr $subsysnr of $totsub\n";

    # Obtain instrument table values from this Obs object.  Break hash tie.
    my $subsys_hdrs = { %{ $subsys_obs->hdrhash } };

    # Need to calculate the frequency information
    $self->calc_freq( $subsys_obs, $subsys_hdrs );

    $self->fill_headers_ACSIS( $subsys_hdrs, $subsys_obs );

    my $error =
      $self->_update_or_insert( 'dbhandle' => $dbh,
                                'table'   => 'ACSIS',
                                'headers' => $subsys_hdrs,
                                'columns' => $cols,
                                'dict'    => $dict,
                              );

    if ($error) {

      $dbh->rollback if $self->load_header_db;
      print "$error\n\n";
      return;
    }

    # Create headers that don't exist
    $self->fill_headers_FILES( $subsys_hdrs, $subsys_obs );

    my $insert_ref = $self->get_insert_values( 'FILES', $cols, $dict, $subsys_hdrs );

    if ( ! $self->update_mode ) {

      $self->insert_hash( 'FILES', $dbh, $insert_ref )
        or $error = $dbh->errstr;
    }

    if ($error) {

      $dbh->rollback;
      print "$error\n\n";
      return;
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

  $status = $enter->insert_hash($table, $dbh, \%to_insert);

=cut

sub insert_hash {

  my ( $self, $table, $dbh, $field_values ) = @_;

  # Go through the hash and work out whether we have multiple inserts
  my @have_ref;
  my $nrows = 1;
  for my $key (keys %$field_values) {

    my $ref = ref $field_values->{$key}
      or next;

    die "Unsupported reference type in insert hash!\n"
      unless $ref eq 'ARRAY';

    my $row_count = scalar @{ $field_values->{$key} };
    if (@have_ref) {

      # count rows
      die "Uneven row count in insert hash ARRAY ref for key '$key'"
        . " ($row_count != $nrows) compared to first key '$have_ref[0]'"
        . "(table $table)\n"
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

  # Get the fields in sorted order (so that we can match with values)
  # and create a template SQL statement. This can be done with the
  # first hash from @insert_hashes

  my @fields = sort keys %{$insert_hashes[0]}; # sort required
  my $sql = sprintf "INSERT INTO %s (%s) VALUES (%s)",
              $table,
              join( ', ', @fields),
              join( ', ', ('?') x scalar @fields )
              ;

  # cache the SQL statement
  my $sth;
  if (!$self->debug && $self->load_header_db) {

    $sth = $dbh->prepare($sql)
      or die "Could not prepare sql statement for insert\n" . $dbh->errstr . "\n" ;
  }

  # and insert all the rows
  for my $row (@insert_hashes) {

    my @values = @{$row}{@fields}; # hash slice

    if ($self->debug) {

      $self->_show_insert_sql( $table, \@fields, \@values );
    } elsif ($self->load_header_db) {

      my $status = $sth->execute(@values);
      return $status if !$status; # return if bad status
    }
  }
  return 1;
}

=item B<update_hash>

Given a table name, a DBI database handle and a hash reference,
retrieve the current data values based on OBSID or OBSID_SUBSYSNR,
decide what has changed and update the values.

  $enter->update_hash( $table, $dbh, \%to_update );

No-op for files table at the present time.

=cut

sub update_hash {

  my ( $self, $table, $dbh, $field_values ) = @_;

  return if $table eq 'FILES';

  # work out which key uniquely identifies the row
  my $unique_key;

  if ($table eq 'COMMON') {

    $unique_key = "obsid";
  } elsif ($table eq 'ACSIS' || $table eq 'FILES') {

    $unique_key = "obsid_subsysnr";
  } else {

    die "Major problem with table name: '$table'\n";
  }

  my $unique_val = $field_values->{$unique_key};

  # run a query with this unique value (but on FILES table more than
  # one entry can be returned - we trap that because FILES for the minute
  # should not need updating

  my $sql = "select * from $table where $unique_key = '$unique_val'";

  my $ref = $dbh->selectall_arrayref( $sql, { Columns=>{} })
    or die "Error retrieving existing content using [$sql]: " . $dbh->errstr;

  die "Only retrieved partial dataset: " . $dbh->errstr
    if $dbh->err;

  # how many rows
  my $count = scalar @{ $ref };

  die "Can only update if the row exists previously! Obsid $unique_val missing.\n"
    if $count == 0;

  die "Should not be possible to have more than one row. Got $count\n"
    if $count > 1;

  my $indb = $ref->[0];

  my %differ;

  for my $key (keys %{$indb}) {

    # since that will update automatically
    next if $key eq 'last_modified';

    if (exists $field_values->{$key}) {

      my $new = $field_values->{$key};
      my $old = $indb->{$key};

      if (defined $new && defined $old) {

        # Dates
        if ($old =~ /\d\d[AP]M$/ && $new =~ /^\d\d\d\d-\d\d-\d\d/) {

          # just assume that if both dates are there they are correct without parsing
          $new = $old;
        }

        if (looks_like_number($new)) {

          if ($new =~ /\./) {

            # floating point
            my $diff = abs($old - $new);
            if ($diff > 0.000001) {

              $differ{$key} = $new;
              # print "$key :Floating point $new != $old ($diff)\n";
            }
          } elsif ($new != $old) {

            $differ{$key} = $new;
          }
        } else {

          # string compare
          if ($new ne $old) {

            $differ{$key} = $new;
          }
        }
      } elsif (defined $new) {

        # not defined currently - inserting new value
        $differ{$key} = $new;
      } elsif (defined $old) {

        # defined in DB but undef in new version - not expecting this but assume this
        # means a null
        $differ{$key} = undef;
      } else {

        # both undef so nothing to do
      }
    }
  }

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

  $sql = sprintf "UPDATE %s SET %s WHERE %s = '%s'",
            $table, $changes, $unique_key, $unique_val;

  if ( $self->debug ) {

    print "$sql\n";
    return 1;
  }

  if ( $self->load_header_db ) {

    my $sth = $dbh->prepare($sql)
      or die "Could not prepare sql statement for insert\n". $dbh->errstr. "\n";

    my $status = $sth->execute( map { $differ{$_} } @sorted );

    return $status if !$status; # return bad status
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

      if ($data_type eq 'datetime' and $val =~ /T/) {

        # print "$column <---- ----> [$val]\n";

        # Convert date to sybase compatible
        my $date = Time::Piece->strptime($val,'%Y-%m-%dT%H:%M:%S');
        $values->{$column} = $date->strftime($self->sybase_date_format);

        printf "Converted date [$val] to [%s] for column [%s]\n", $values->{$column}, $column
          if $self->debug;

      } elsif (exists $transform_data{$data_type}) {

        if (exists $transform_data{$data_type}{$val}) {

          # This value needs to be transformed to the new value
          # defined in the %transform_data hash
          $values->{$column} = $transform_data{$data_type}{$val};

          printf "Transformed value [$val] to [%s] for column [%s]\n", $values->{$column}, $column
            if $self->debug;
        }
      } elsif ($column eq 'lststart' or $column eq 'lstend') {

        # Convert LSTSTART and LSTEND to decimal hours
        my $ha = new Astro::Coords::Angle::Hour($val, units => 'sex');
        $values->{$column} = $ha->hours;

        printf "Converted time [$val] to [%s] for column [%s]\n", $values->{$column}, $column
          if $self->debug;
      }
    }
  }
  return 1;
}

=item B<create_headers>

Create any headers that need to go into the database, but don't exist.
Currently these headers are:

  decj2000, decj2000_int, filename, idkey, raj2000, raj2000_int,
  ut_dmf, (and sometimes) RUN, UTDATE EXPOSED (out of date)

These arguments should be provided in this order: name of the table to
receive the headers, an L<OMP::Info::Obs> object, and a reference to the
header hash.

  $enter->create_headers( $common_table, $obs, \%headers );

=cut

BEGIN
{
  my @db_table = qw[ COMMON FILES ACSIS ];

  sub create_headers {

    my ( $self, $table, $obs, $header ) = @_;

    my $filler = first { $table eq $_ } @db_table
                  or throw JSA::Error "'$table' is unknown\n";

    $filler = join '_', 'fill_headers', $filler;

warn $filler;

    return $self->$filler( $header, $obs );
  }
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
  my $release_date;
  if ($obs->isScience) {

    # semester release
    my $semester = OMP::General->determine_semester( date => $obsdate,
                                                      tel => 'JCMT'
                                                    );
    my ($sem_begin, $sem_end) =
      OMP::General->semester_boundary( semester => $semester, tel => 'JCMT' );

    # Use DateTime so that we can have proper addition. Add 1 year 1 day because
    # sem_end refers to the UT date and doesn't specify hours/minutes/seconds
    $release_date =
        DateTime->from_epoch( epoch => $sem_end->epoch, time_zone => 'UTC' )
      + DateTime::Duration->new( years => 1, hours => 23, minutes => 59, seconds => 59 );

  } else {

    # immediate release
    $release_date = OMP::General->yesterday(1);
  }
  $header->{'release_date'} = $release_date->strftime($self->sybase_date_format);

  print "Created header [release_date] with value [%s]\n", $header->{'release_date'}
    if $self->debug;

  # Create last_modified
  my $today = gmtime;
  $header->{'last_modified'} = $today->strftime($self->sybase_date_format);

  print "Created header [last_modified] with value [%s]\n", $header->{'last_modified'}
    if $self->debug;

  return;
}

=item B<fill_headers_FILES>

Fills in the headers for C<FILES> database table, given a headers
hash reference and an L<OMP::Info::Obs> object.

  $enter->fill_headers_FILES( \%header, $obs );

=cut

sub fill_headers_FILES {

  my ( $self, $header, $obs ) = @_;

  my $obsid = $obs->obsid;

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

      die "Internal error - NSUBSCAN does not exist yet there is only one file!";
    }
  }

  printf "Created header [file_id] with value [%s]\n", join ',', @{ $header->{'file_id'} }
    if $self->debug;

  return $self->_fill_headers_obsid_subsys( $header, $obsid );
}

=item B<fill_headers_ACSIS>

Fills in the headers for C<ACSIS> database table, given a headers
hash reference and an L<OMP::Info::Obs> object.

  $enter->fill_headers_ACSIS( \%header, $obs );

=cut

sub fill_headers_ACSIS {

  my ( $self, $header, $obs ) = @_;

  my $obsid = $obs->obsid;

  # Create max_subscan
  my @subscans = $obs->simple_filename;
  $header->{'max_subscan'} = scalar @subscans;

  printf "Created header [max_subscan] with value [%s]\n", $header->{'max_subscan'}
    if $self->debug;

  return $self->_fill_headers_obsid_subsys( $header, $obsid );
}

# Create obsid_subsysnr
sub _fill_headers_obsid_subsys {

  my ( $self, $header, $obsid ) = @_;

  # Create obsid_subsysnr
  $header->{'obsid_subsysnr'} = join '_', $obsid,  $header->{'SUBSYSNR'};

  printf "Created header [obsid_subsysnr] with value [%s]\n", $header->{'obsid_subsysnr'}
    if $self->debug;

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

Given a table name, a hash reference containing table column
information (see global hash %columns), a hash reference containing
the dictionary contents, and a hash reference containing observation
headers, return a hash reference with the table's columns as the keys,
and the insertion values as the values.

  $vals =
    $enter->get_insert_values( $table, \%columns, \%dictionary, \%hdrhash );

=cut

sub get_insert_values {

  my ( $self, $table, $columns, $dictionary, $hdrhash ) = @_;

  # Map headers to columns, translating from the dictionary as
  # necessary.
  my %values;
  for my $header (keys %$hdrhash) {
    if (exists $columns->{$table}{lc($header)}) {

      # Column exists, no need to translate
      $values{lc($header)} = $hdrhash->{$header};
    } else {

      # Header not found, try translating
      if (exists $dictionary->{lc($header)} and exists $columns->{$table}{$dictionary->{lc($header)}}) {

        # Found header alias in dictionary and column exists in table
        my $alias = $dictionary->{lc($header)};
        $values{$alias} = $hdrhash->{$header};

        print "Mapped header [$header] to column [$alias]\n"
          if $self->debug;
      }
    }
    print "Could not find alias for header [$header]. Skipped.\n"
      if $self->debug and ! exists $values{lc($header)};
  }

  # Do value transformation
  $self->transform_value($table, $columns, \%values);

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

  my $sth = $dbh->prepare_cached("select max(idkey) from $table");
  $sth->execute
    or die sprintf "Could not obtain max idkey: %s\n", $dbh->errstr;

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

  open my $DICT, '<', $dictionary
    or die "Could not open data dictionary '$dictionary': $!\n";

  my @defs = grep { $_ !~ /^\s*(?:#|$)/ } <$DICT>;  # Slurp!

  close $DICT
    or die "Error closing data dictionary '$dictionary': $!\n";

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

  $status = $enter->calc_radec( $obs, $header );

=cut

sub calc_radec {

  my ( $self, $obs, $headerref ) = @_;

  # Filenames for a subsystem
  my @filenames = $obs->filename;

  # Now need to write these files to  temp file
  my $fh = new File::Temp();
  print $fh map { $_ ."\n" } @filenames;
  close($fh);

  # PA (may not be present)
  my $pa = $headerref->{MAP_PA};
  $pa *= -1 if defined $pa;

  # command depends on ACSIS or SCUBA-2
  # turn off autogrid - only rotate raster maps. Just need bounds.
  my $systat =
    system( '/star/bin/smurf/makecube',
              "in=^$fh",
              'system=ICRS',
              'out=!',
              'pixsize=1',
              'autogrid=no',
              'msg_filter=quiet',
              (defined $pa ? "crota=$pa" : () ),
              'reset'
          );

  if ($systat == 256) {

    # ADAM_EXIT - there will be a standard out message so the user should follow up
    return 0;
  } elsif ($systat != 0) {

    # probably control-C
    die "Error running SMURF: Status = $systat";
  }

  # Get the bounds
  my %result;
  for my $k ( qw/ FTL FBR FTR FBL /) {

    my $res = qx{ /star/bin/kappa/parget $k makecube };
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

  my (undef, %state ) = $self->read_ndf( $filenames[0], qw/ TCS_TR_SYS / );
  die "Error reading state information from file $filenames[0]\n"
    unless keys %state;

  # check for APP or AZEL (should never be AZEL!)
  if ($state{TCS_TR_SYS} !~ /^(APP|AZ)/) {

    for my $k (qw/ REFLON REFLAT / ) {

      my $res = qx{ /star/bin/kappa/parget $k makecube };
      chomp($res);
      $result{$k} = $res;
    }

    # convert to radians
    $result{REFLON} = Astro::Coords::Angle::Hour->new( $result{REFLON}, units => 'sex', range => '2PI' )->degrees;
    $result{REFLAT} = Astro::Coords::Angle->new( $result{REFLAT}, units => 'sex', range => 'PI' )->degrees;

  } else {

    $result{REFLON} = undef;
    $result{REFLAT} = undef;
  }

  $headerref->{obsra} = $result{REFLON};
  $headerref->{obsdec} = $result{REFLAT};

  return 1;
}

=item B<calc_freq>

Calculate frequency properties, updates given hash reference.

  $enter->calc_freq( $obs, $headerref );

It Calculates:
    zsource, restfreq
    freq_sig_lower, freq_sig_upper : BARYCENTRIC Frequency GHz
    freq_img_lower, freq_img_upper : BARYCENTRIC Frequency Image Sideband GHz

=cut

sub calc_freq {

  my ( $self, $obs, $headerref ) = @_;

  # Filenames for a subsystem
  my @filenames = $obs->filename;

  # need the Frameset
  my $wcs = $self->read_ndf( $filenames[0] );

  # Change to BARYCENTRIC, GHz
  $wcs->Set( 'system(1)' => 'FREQ',
             'unit(1)' => 'GHz',
             stdofrest => 'BARY' );

  # Rest Frequency
  $headerref->{restfreq} = $wcs->Get( "restfreq" );

  # Source velocity
  $wcs->Set( sourcesys => 'redshift' );
  $headerref->{zsource} = $wcs->Get( "sourcevel" );

  # Upper and lower values require that we know the GRID bounds
  my @x = (1, $headerref->{NCHNSUBS});

  # need some dummy data for axis 2 and 3 (or else some code to split the
  # specFrame)
  my @y = (1,1);
  my @z = (1,1);

  my @observed = $wcs->TranP( 1, \@x, \@y, \@z );

  # now need to switch to image sideband (if possible) (some buggy data is not
  # setup as a DSBSpecFrame)
  my @image;
  eval {
    my $sb = uc($wcs->Get("SideBand"));
    $wcs->Set( 'SideBand' => ($sb eq 'LSB' ? 'USB' : 'LSB' ) );

    @image = $wcs->TranP( 1, \@x, \@y, \@z );
  };

  # need to sort the numbers
  my @freq = sort { $a <=> $b } @{ $observed[0] };
  $headerref->{freq_sig_lower} = $freq[0];
  $headerref->{freq_sig_upper} = $freq[1];

  if (@image && @{$image[0]}) {

    @freq = sort { $a <=> $b } @{ $image[0] };
    $headerref->{freq_img_lower} = $freq[0];
    $headerref->{freq_img_upper} = $freq[1];
  }

  return;
}

=item B<read_ndf>

Open an NDF file, read the frameset and the first entry from the
supplied list of JCMTSTATE components (can be empty).

Returns hash of JCMTSTATE information and the Starlink::AST object.

  ($wcs, %state) = $enter->read_ndf( $file, @state );

returns empty list on error.  In scalar context just returns WCS
frameset...

  $wcs = $enter->read_ndf( $file );

=cut

sub read_ndf {

  my ( $self, $file, @statekeys ) = @_;

  my $status = &NDF::SAI__OK;
  my $bad;

  # begin context and open file
  err_begin( $status );
  ndf_begin( );
  ndf_find( NDF::DAT__ROOT, $file, my $indf, $status);

  # read the WCS
  my $wcs = ndfGtwcs( $indf, $status );

  # Get extension
  my %state;
  if (@statekeys) {

    ndf_xloc( $indf, "JCMTSTATE", "READ", my $sloc, $status);
    for my $k (@statekeys) {

      dat_find( $sloc, $k, my $lloc, $status);
      dat_type( $lloc, my $type, $status);
      dat_size( $lloc, my $size, $status);

      if ($status == &NDF::SAI__OK) {

        my @values;
        if ($type =~ /_CHAR/) {

          dat_getvc($lloc, $size, @values, my $el, $status );
        } elsif ($type =~ /^_[IUBL]/) {

          dat_getvi( $lloc, $size, @values, my $el, $status );
        } elsif ($type =~ /^_[DR]/) {

          dat_getvd( $lloc, $size, @values, my $el, $status );
        } else {

          if ($status == &NDF::SAI__OK) {

            $status = &NDF::SAI__ERROR;
            err_rep( " ", "Error with type $type", $status);
          }
        }
        if ($status == &NDF::SAI__OK) {

          $state{$k} = $values[0]; # first only
        }
      }
    }
  }

  # tidy up
  if ($status != &NDF::SAI__OK) {

    err_flush( $status );
    $bad = 1;
  }

  err_end( $status );
  ndf_end($status);

  return () if $bad;
  return wantarray ? ( $wcs, %state ) : $wcs;
}

=item B<_show_insert_sql>

Prints insert SQL statement, given table name, column names, and
column values.

  $enter->_show_insert_sql( 'FILES', \@names, \@values );

=cut

sub _show_insert_sql {

  my ( $self, $table, $fields, $values ) = @_;

  # print out some SQL that is not going to be executed
  for ( @{ $values } ) {

    unless ( defined $_ ) {

      $_ = 'NULL';
    } else {

      $_ = "\'$_\'"
        if /([a-zA-Z]|\s+)/ and $_ !~ /e\+/ ;
    }
  }

  printf "-----> SQL: INSERT INTO %s (%s) VALUES (%s)\n",
      $table,
      join( ', ', @{ $fields } ),
      join( ', ', @{ $values } );

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

  # Create missing headers.
  #$self->create_headers( @args{qw/ table obs headers /} );

  my $run = $self->update_mode ? 'update_hash' : 'insert_hash';

  my $ok =
    $self->$run( @args{qw/ table dbhandle /},
                  $self->get_insert_values( @args{qw/ table columns dict headers /} )
                );

  return $args{'dbhandle'}->errstr;
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

