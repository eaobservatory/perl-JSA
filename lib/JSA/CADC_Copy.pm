package JSA::CADC_Copy;

use warnings;
use strict;

#use File::Basename;
use File::Spec;
use Scalar::Util qw[ blessed looks_like_number ];
use Time::Piece;
use Time::Seconds qw[ ONE_DAY ];
use List::MoreUtils qw[ any ];

use JSA::Command qw/ run_command /;
use JSA::Error qw[ :try ];
use OMP::Config;

BEGIN {

  my %inst =
    (
      'ACSIS' =>
        {
          'ok-regex' =>
            qr[^\. a
                \d{8}   # Date.
                _
                (\d{5}) # Observation number.
                \.ok$
              ]x,

          'root' =>
            '/jcmtdata/raw/acsis/spectra',
        },

      'SCUBA2' =>
        {
          'ok-regex' => qr/^ s[48][a-d] \d{8} _ (\d{5}) [.]ok $/x,
          'root' =>
            #'/jcmtdata/raw/scuba2/ok/eng/',
            '/jcmtdata/raw/scuba2',
        },
    );

  $inst{'ACSIS'}->{'path-regex'} =
    qr{ ^
        # Parent instrument directory with date.
        ( .+?
          /
          \d{8}
        )
        [/\d]+?
        # Base file name.
        (
          ([ah])
          \d{8} _
          # Observation number.
          ( \d{5} )
          _ \d{2} _ \d{4} [.]sdf
        )
        $
      }x;

  $inst{'SCUBA2'}->{'path-regex'} =
    qr{ ^
        # Parent instrument directory with date.
        ( .+?
          /
          \d{8}
        )
        [/\d]+?
        # Base file name.
        (
          (s[48][a-d])
          \d{8} _
          # Observation number.
          ( \d{5} )
          _ \d{4} [.]sdf
        )
        $
      }x;
  # Instruments to loop through
  my @inst = sort { lc $a cmp lc $b } keys %inst;

  sub instrument_list {

    my ( $class ) = shift @_;

    return @inst
      unless scalar @_;

    @inst = @_;
    return;
  }

  # Make instrument_root(), instrument_ok_regex(), instrument_path_regex
  # accessors with rudimentary checking.
  for my $prop ( keys %{ $inst{'ACSIS'} } ) {

    ( my $sub = $prop ) =~ tr/-/_/;

    $sub = join '_', 'instrument', $sub;
    no strict 'refs';
    *$sub =
      sub {
            my ( $self, $inst ) = @_;

            throw JSA::Error::BadArgs 'No instrument given.'
              unless defined $inst;

            $inst = uc $inst;

            throw JSA::Error::BadArgs "Unknown instrument, '$inst', given."
              unless exists $inst{ $inst };

            shift, shift;

            return $inst{ $inst }->{ $prop }
              unless scalar @_;

            my $val = shift;
            throw JSA::Error::BadArgs "No $prop given."
              unless defined $val;

            $inst{ $inst }->{ $prop } = $val;
            return;
          };
  }

  my %default =
    (
      # If 'date' is specified, its is used for start- & end-date if any of the
      # two are not.  Else, if end-date is not given, current UT date is used
      # (see find_start_end_dates); if start-date is missing, then it is
      # searched for going back no more than 30 days (see find_start_dates()).
      'date'       => undef,
      'start-date' => undef,
      'end-date'   => undef,

      'verbose' => 0,
      'dry-run' => 0,

      # Indicator if to force upload.
      'force' => undef,

      'cadc-dir' =>
        '/jcmtdata/cadc/new',
    ) ;

  # Make cadc_dir(), dry_run(), force(), verbose() accessors.
  for my $key ( keys %default ) {

    next
      # Need to verify a date.
      if $key =~ /\bdate\b/
      # Need to verify that value given would be an integer.
      or $key eq 'verbose' ;

    ( my $sub = $key ) =~ tr/-/_/;
    no strict 'refs';
    *$sub = sub {

              my $self = shift @_;
              return $self->{ $key } unless scalar @_;

              $self->{ $key } = shift @_;
              return;
            };
  }

  # Make date(), start_date(), & end_date() methods.
  for my $key ( grep { /\bdate\b/ } keys %default ) {

    ( my $sub = $key ) =~ tr/-/_/;
    no strict 'refs';
    *$sub =
      sub {
            my $self = shift @_;
            return $self->{ $key } unless scalar @_;

            my ( $date ) = @_;

            __PACKAGE__->check_date( $date );

            $self->{ $key } =
              $date =~ /^\d{8}$/
              ? Time::Piece->strptime( $date, $self->get_date_format )
              : $date
              ;

            return;
          };
  }

  # Constructor, uses keys from %default & related subs.
  sub new {

    my ( $class, %args ) = @_;

    my $obj = bless { %default }, $class;

    for my $k ( keys %args ) {

      next unless exists $default{ $k };

      ( my $sub = $k ) =~ tr/-/_/;

      throw JSA::Error::FatalError "None such method: $sub"
        unless $obj->can ( $sub );

      $obj->$sub( $args{ $k } );
    }

    return $obj;
  }

}

# Given a date value, returns true if it is a Time::Piece object or
# matches the number of digits in a date specified in 'YYYYMMDD'
# format.
sub check_date {

  my ( $class, $date ) = @_;

  if ( defined $date ) {

    return
      if $date =~ /^\d{8}$/
      or ( blessed $date && blessed $date eq 'Time::Piece' );
  }

  throw JSA::Error::BadArgs
    'Must provide date in "YYYYMMDD" format or as Time::Piece object.';
}

# Returns the date format.
sub get_date_format {

  my ( $class ) = @_;

  return '%Y%m%d';
}

# Verbosity indicator accessor.
sub verbose {

  my $self = shift @_;
  return $self->{'verbose'} unless scalar @_;

  my ( $val ) = @_;

  throw JSA::Error::BadArgs "$val is not a number."
    if $val && ! looks_like_number( $val );

  $self->{'verbose'} = $val;
  return;
}

# From start date to end date, go through each directory creating symbolic links
# to each data file.  Once the link has been created, create a .cadc_ok file to
# indicate that the file should be ignored in the future.  Do this for each
# instrument in the $self->instrument_list().
sub upload_per_instrument {

  my ( $self ) = @_;

  my ( $starts, $end ) = $self->find_start_end_dates;

  my $okfiles = $self->okfiles;
  my $format = __PACKAGE__->get_date_format;

  for my $inst ( $self->instrument_list ) {

    # The current date in the loop
    my $inst_start = $starts->{ $inst };

    my @bound = ( $inst_start->strftime( $format ),
                  join ' ', $end->strftime( $format ), '23:59:59'
                );

    # Used to filter out the files for which there is no *.ok file elsewhere.
    my $file_ids = $self->get_file_ids( @bound );

    unless ( $file_ids ) {

      $self->verbose
        and printf "Nothing found between $bound[0] - $bound[1] for instrument $inst.\n";

      next;
    }

    while ( $inst_start->epoch <= $end->epoch ) {

      my $ymd = $inst_start->strftime( $format );
      my $src = $self->make_inst_date_path( $inst, $ymd );

      if ( -e $src ) {

        # Slurp up the *.ok files if we didn't do so earlier while obtaining the
        # start date.
        unless ( exists $okfiles->{ $inst }->{ $ymd } ) {

          my $return = __PACKAGE__->read_okfiles( $src );
          $okfiles->{ $inst }->{ $ymd } = $return->{'ok'}
        }

        $self->upload_make_cadc_ok( 'instrument' => $inst,
                                    'source-dir' => $src,
                                    'okfiles' => $okfiles->{ $inst }->{ $ymd },
                                    'file-ids' => $file_ids,
                                  );
      }

      $inst_start += ONE_DAY;
    }
  }

  return;
}

# Returns an array reference of array references containing base file names (aka
# file ids).  Returns nothing if SQL query finds nothing.
sub get_file_ids {

  my ( $self, $start, $end ) = @_;

  my $time_re = qr/^ \d{8} [ T] \d{2}:\d{2}:\d{2} $/x;

  throw JSA::Error::BadArgs qq[Need start & end dates in "yyyymmddThh:mm:ss" format.\n]
    unless defined $start
    and    defined $end
    and $start =~ m/$time_re/
    and $end   =~ m/$time_re/
    ;

  ( my $sql =
    q[ SELECT f.file_id
        FROM COMMON c, FILES f
        WHERE c.obsid = f.obsid
          AND c.date_obs >= ?
          AND c.date_obs <= ?
      ]
  ) =~ s/[ ]{2,}/ /g;

  # Connect to the CADC mirror DB
  require OMP::DBbackend::Archive;
  my $back = OMP::DBbackend::Archive->new;
  my $dbh = $back->handle;

  my $trace =
    3 < $self->verbose
    ? # Heavy trace.
      2
    : 2 < $self->verbose
      ? # Light trace.
        1
      : # No trace.
        0 ;

  $dbh->trace( $trace );

  my $files = $dbh->selectall_arrayref( $sql, {}, $start, $end )
    or throw JSA::Error::FatalError
        sprintf "Could not perform DB query: %s\n", $dbh->errstr;

  return unless $files or scalar @{ $files };

  return [ map { @$_ } @{ $files } ];
}

# Returns a hash reference with the keys 'ok' and 'cadc_ok', where the value of
# each key is a reference to an array of '.ok' or '.cacdc_ok' file names.
sub read_okfiles {

  my ( $class, $dir ) = @_;

  opendir my $dh, $dir
    or throw JSA::Error::FatalError "Can't open directory [$dir]: $!\n";

  my %ok;
  my $ok = qr[^\. .*? \. ( (?:cadc_)? ok )$]x;

  while ( my $file = readdir( $dh ) ) {

    next unless $file =~ /$ok/;

    push @{ $ok{ "$1" } }, $file;
  }

  closedir $dh
    or throw JSA::Error::FatalError "Can't close directory [$dir]: $!\n";

  return \%ok;
}

# Loop through normal .ok files, and create links for data files that
# do not have a corresponding .cadc_ok file.  It takes argument as a hash ...
#   instrument => ACSIS | SCUBA2
#   source-dir => /inst/date/dir
#   obs-date   => 20080820        -- used to generate source directory name
#   okfiles    => \@okfiles_per_inst_per_date
#   file-ids   => \@file_id_in_db
sub upload_make_cadc_ok {

  my ( $self, %args ) = @_;

  my $inst = $args{'instrument'}
    or throw JSA::Error::BadArgs "No instrument given.\n";

  throw JSA::Error::BadArgs "Neither source directory nor obs date given\n"
    unless $args{'source-dir'}
    or $args{'obs-date'} ;

  my $inst_date =
    $args{'source-dir'}
    || $self->make_inst_date_path( $inst, $args{'obs-date'} );
    ;

  my %file_id = map { $_ ? ( $_ => undef ) : () } @{ $args{'file-ids'} };

  for my $ok ( @{ $args{'okfiles'} } ) {

    my $cadc_ok = __PACKAGE__->make_cadc_ok_path( $ok, $inst_date );

    if ( -e $cadc_ok && ! $self->force ) {

      $self->verbose and print "$cadc_ok exists. Skipping.\n";

      next;
    }

    my $obsnum = $self->make_obsnum_path( $inst, $inst_date, $ok );

    my @files = @{ __PACKAGE__->filter_files( $obsnum, \%file_id ) };
    if ( scalar @files ) {

      $self->make_symlink( $obsnum, "$_" ) for @files;

      $self->make_empty_file( $cadc_ok );
    }
  }

  return;
}

# Returns the UT date directory path given the instrument name.
sub make_inst_date_path {

  my ( $self, $inst, $date ) = @_;
  return File::Spec->catdir( $self->instrument_root( $inst ), $date );
}

# Returns a *.cadc_ok file name given a *.ok base name & the parent directory.
sub make_cadc_ok_path {

  my ( $class, $ok, $parent ) = @_;

  ( my $cadc_ok = $ok ) =~ s/\.ok/\.cadc_ok/
    or throw JSA::Error::FatalError "Failed to genetate '*.cadc_ok' from '$ok'\n";

  return File::Spec->catfile( $parent, $cadc_ok );
}

# Returns a observation directory name given a *.ok base name; the parent
# directory; and the intrument name (to extract observation number from a
# F<*.ok> file).
sub make_obsnum_path {

  my ( $self, $inst, $parent, $ok ) = @_;

  my $re = $self->instrument_ok_regex( $inst ) ;
  my ( $obsnum ) = $ok =~ /$re/
    or throw JSA::Error::FatalError "File name, $ok, does not match regexp.\n";

  return File::Spec->catdir( $parent, $obsnum );
}

# Returns the files in the given directory that have been replicated to the CADC
# mirror db specified by the hash reference.
sub filter_files {

  my ( $class, $dir, $db_ok ) = @_;

  opendir my $dh, $dir
    or throw JSA::Error::FatalError
        sprintf "Could not open directory [%s]: %s\n", $dir, $!;

  my @files = grep { exists $db_ok->{ $_ } } readdir $dh;

  closedir $dh;

  return \@files;
}

# Makes a symbolic link for given source directory and the observation number
# string.
sub make_symlink {

  my ( $self, $obsnum, $base ) = @_;

  my $link;
  ( $obsnum, $link) =
      map { File::Spec->catfile( "$_", $base ) } $obsnum, $self->cadc_dir ;


  my $message = "Created link: $link -> $obsnum\n";

  if ( $self->dry_run ) {

    print $message;
    return;
  }

  symlink $obsnum, $link
    or throw JSA::Error::FatalError
        sprintf "Can't create symlink %s to %s: %s.\n", $link, $obsnum, $!;

  $self->verbose and print $message;

  return;
}

# Creates an empty file at the given path.
sub make_empty_file {

  my ( $self, $path ) = @_;

  return if -e $path;

  if ( $self->dry_run ) {

    print "Created $path\n";
    return;
  }

  umask 0117;

  open my $fh, '>', $path
    or throw JSA::Error::FatalError "Could not create [$path] file: $!\n";
  close $fh;

  $self->verbose and print "Created [$path]\n";

  return;
}

# Returns a hash reference with instrument as keys & start date as values, and
# an end date.
sub find_start_end_dates {

  my ( $self ) = @_;

  my ( $date, $start, $end ) =
    map { $self->$_ } qw[ date start_date end_date ];

  #  Override start- & end-date.
  $start = $end = $date if $date;

  $end = gmtime unless $end;

  # (Re)set dates for use elsewhere.
  $self->start_date( $start ) if $start;
  $self->end_date( $end );

  my $start_inst =
    $start
    ? { map { $_ => $start } $self->instrument_list }
    : # The start date can be different for each instrument.
      $self->find_start_dates
      ;

  return ( $start_inst, $end );
}

# Returns a hash reference of instruments as keys and start dates as values.
#
# Determines the start date by stepping backwards one day at a time until we've
# reached a UT-date directory containing a .cadc_ok file.
sub find_start_dates {

  my ( $self ) = @_;

  my $end = $self->end_date;

  throw JSA::Error::BadArgs 'No end date is specified.'
    unless $end;

  my $okfiles = $self->okfiles;
  my %start;
  for my $inst ( $self->instrument_list ) {

    # Current date.
    my $date = $end;

    # Keep track of how many days we've gone back>
    my $days;
    until ( $start{ $inst } ) {

      $days++;

      # Could not find a single .cadc_ok file.
      throw JSA::Error::FatalError
              "Could not find any .cadc_ok files, unable to determine where to begin.\n"
              . "Try the 'start_date' option."
        if $days >= 30 ;

      my $ymd = $date->strftime( $self->get_date_format );
      my $src_dir = $self->make_inst_date_path( $self->instrument_root( $inst ), $ymd );

      if ( -e $src_dir ) {

        my $return = __PACKAGE__->read_okfiles( $src_dir );
        $okfiles->{ $inst }->{ $ymd } = $return->{'ok'};

        $start{ $inst } = $date
          if defined $return->{'cadc_ok'}->[0];
      }

      $date -= ONE_DAY;
    }
  }

  return \%start;
}

# To pass same hash reference among various subs.
{
  my $okfiles;

  # Returns the hash reference, to be manipulated by others.
  sub okfiles {

    $okfiles ||= {};
    return $okfiles;
  }

  sub clear_okfiles {

    $okfiles = {};
    return;
  }
}

# This function returns a hash reference, with keys being the files at CADC. If
# the UT date is in an incorrect format, this method returns undef. If no files
# are returned, this method returns an empty hash reference.
#
# Accepts also an optional list of instrument prefixes, out of...
#
#   a,
#   s4a,
#   s4b,
#   s4c,
#   s4d,
#   s8a,
#   s8b,
#   s8c,
#   s8d
sub at_cadc {

  my ( $ut, @prefix ) = @_;

  return if defined $ut && $ut !~ /^\d{8}$/;

  my @inst = qw/ a s4a s4b s4c s4d s8a s8b s8c s8d /;

  # Use instrument prefix and the date.
  if ( $ut ) {

    @prefix = @inst unless scalar @prefix;

    return _check_cadc( undef, map { "${_}${ut}" } @prefix );
  }

  # Assume to be file names.
  my @file;
  for my $f ( @prefix ) {

    push @file, $f
      if any { $f =~ /^$_\d{8}/ } @inst;
  }

  return _check_cadc( undef, @file );
}

sub _check_cadc {

  my ( $wait, @prefix ) = @_;

  return unless scalar @prefix;

  # Time to wait for a random, reasonable amount.
  $wait ||= 20;

  my @curl = (qw[ curl --silent --location ]);
  my $cadc_url = 'http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/cadcbin/jcmtInfo?file=';

  # To avoid hammering the server when run multiple times in a row.
  my $sleepy_time = scalar( @prefix ) -1;

  # Go through each instrument prefix and push the list of files onto
  # our array.
  my @uploaded;
  foreach my $prefix ( @prefix ) {

    # Try to use curl with a URL (which is what jcmtInfo does anyhow).  Use a
    # sybase wildcard to get all matching files.
    my $url = sprintf '%s%s%%', $cadc_url, $prefix;

    my ($stdout, $stderr, $stat) = run_command( @curl, $url);

    push @uploaded, _filter_curl_output( $stdout, $stat );

    $sleepy_time-- > 0 and sleep $wait;
  }

  my %at_cadc = map { chomp( $_ ); $_ => undef } @uploaded;

  return \%at_cadc;
}

sub _filter_curl_output {

  my ( $files, $stat ) = @_;

  return if $stat != 0;
  return unless @{ $files };
  return if $files->[0] =~ /No such file/;

  return @{ $files };
}

1;

=pod

=head1 NAME

JSA::CADC_Copy - Create links to JCMT data in CADC e-transfer staging area

=head1 SYNOPSIS

Create an object:

  my $copy = JSA::CADC_Copy->new;


Shows some progress of the process:

  $copt->verbose( 1 ) ;


Upload for current UT date:

  $copy->upload_per_instrument;


or, upload for a particular date:

  $copy->date( '20080820' );
  $copy->upload_per_instrument;


or, upload for a date range:

  $copy->start_date( '20080820' );
  $copy->end_date(   '20080821' );
  $copy->upload_per_instrument;


Force an upload, overwriting existing symbolic links & F<*.cadc_ok>
files:

  $copy->force( 1 );
  $copy->upload_per_instrument;


Or, just create the object with options:

  $copy = JSA::CADC_Copy->new(
                               'force'   => 0,
                               'verbose' => 1,
                               'start-date' => '20080820',
                               'end-date'   => '20080821',
                              );

  $copy->upload_per_instrument;


=head1 DESCRIPTION

This module prepares data to be transferred to the CADC by creating
links from the data to the staging area.  It creates F<*.cadc_ok>
files to skip related data files in future.

=head2 FUNCTIONS

=over 2

=item B<at_cadc>

Return a list of files uploaded to CADC for a specific UT date.

  my $at_cadc = at_cadc( $ut );

The UT date must be in YYYYMMDD format. This function returns a hash
reference, with keys being the files at CADC. If the UT date is in an
incorrect format, this method returns undef. If no files are returned,
this method returns an empty hash reference.

Return a list of files uploaded to CADC for a specific UT date.
Accepts also an optional list of instrument prefixes, out of...

  a,
  s4a,
  s4b,
  s4c,
  s4d,
  s8a,
  s8b,
  s8c,
  s8d


If no list is given, all of the above prefixes are used.

  my $at_cadc = at_cadc( $ut, qw[ s4a s8d ] );

=back

=head2 CLASS METHODS

=over 4

=item B<new>

This is the constructor method, which accepts I<cadc-dir>, I<date>,
I<start-date>, and I<end-date> as options as a "hash".

=over 2

=item I<cadc-dir>

Specify the directory where symbolic needs to be made for upload.
Default is F</jcmtdata/cadc/new>.

=item I<date>

Specify the date of data to be linked, either as a L<Time::Piece>
object or a string in C<YYYYMMDD> format.

If given, it overrides given I<start-> or I<end-date>.

=item B<dry_run>

Show what would have been done, without actually moving or creating files.

=item I<end-date>

Specify the end date of data to be linked, representation as specified
for I<date>.

=item I<force>

Specify to a true value to force upload (overwrite existing files).
Default is a false value.

=item I<start-date>

Specify the start date of data to be linked, representation as
specified for I<date>.

=back

  $copy = JSA::CADC_Copy->new( 'cadc-dir' => '/non/default/path' );


=item B<check_date>

Given a date value, returns nothing if it is a Time::Piece object or
matches the number of digits in a date specified in 'YYYYMMDD' format.
Else, throws L<JSA::Error::BadArgs> execption.

  for my $date ( 20080820 2008 ) {

      try {

        JSA::CADC_Copy->check_date( $date );
      }
      catch with JSA::Error::BadArgs {

        my $e = shift @_;
        print "$date : $e\n";
      }
  }


=item B<filter_files>

Given a directory path and a hash reference of files as keys to keep,
returns the file names which exist in the directory as an array
reference.

Throws JSA::Error::FatalError if the directory cannot be opened.

  $filtered = JSA::CADC_Copy->filter_files( '/dir/path',
                                            { 'file-name' -> undef }
                                          );


=item B<force>

Returns the currently set value if no arguments are specified.

Else, sets the value to the given true value.  If true, existing files
will be overwritten to force upload.

  $copy->force( 'yes' );
  ...
  do { ... } if $copy->force;


=item B<get_date_format>

Returns the date format suitable for C<strftime> format ('%Y%m%d').

  print Time::Piece->strftime( '20080820',
                                JSA::CADC_Copy->get_date_format,
                              );


=item B<instrument_list>

Returns a case insensitive sorted list of instrument names.

  @inst = $copy->instrument_list;


=item B<make_cadc_ok_path>

Returns a F<*.cadc_ok> file name given a F<*.ok> base name & the
parent directory.

  $cadc_ok = JSA::CADC_Copy->make_cadc_ok_path( '.a_20080920_0123.ok',
                                                 '/parent'
                                                );


=item B<make_empty_file>

Given a file path, creates an empty file.  Nothing is done if the path
already exists.

Throws JSA::Error::FatalError if the file cannot be created.

  $copy->make_empty_file( '/file/path' );


=item B<make_symlink>

Creates the link in CADC staging area, given observation number
direcotry path and the base file name in there.

Throws JSA::Error::FatalError if the symbolic link cannot be created.

  $copy->make_symlink( '/path/to/obs', 'a_20080820_00820_08_020.sdf' );


=item B<read_okfiles>

Given a directory name, returns a hash reference with the keys 'ok'
and 'cadc_ok', where the value of each key is an array reference of
'.ok' or '.cacdc_ok' file names respectively.

  $files = JSA::CADC_Copy->read_okfiles( $directory );

  $ok      = $files->{'ok'};
  @cadc_ok = @{$files->{'cadc_ok'}};


=back

=head2 INSTANCE METHODS

=over 4

=item B<cadc_dir>

Returns the directory name in which to create F<*.cadc_ok> files if no
arguments are given.

  $dir = $copy->cadc_dir;

Sets the directory to the given argument, returns nothing.

  $copy->cadc_dir( '/path/for/cadc/new' );

=item B<clear_okfiles>

Set the hash reference (returned by I<okfiles> method) to be an empty
one.

  $copy->clear_okfiles;

=item B<date>

Returns the date of data to be linked if no argument is given.

Else, sets the date to the given argument, and returns nothing.  Date
should either be a L<Time::Piece> object or a string in C<YYYYMMDD>
format.

  # Set a date.
  $copy->date( '20080820' );

  # Retrieve it.
  $date = $copy->date;

=item B<end_date>

Returns the end date of data to be linked if no argument is given.

Else, sets the end date to the given argument, and returns nothing.
Date should either be a L<Time::Piece> object or a string in
C<YYYYMMDD> format.

  # Set a date.
  $copy->end_date( '20080820' );

  # Retrieve it.
  $date = $copy->end_date;


=item B<find_start_dates>

Returns a hash reference of instruments as keys and start dates as
values.

It is called by C<find_start_end_dates()> method when neither
I<start-date> nor I<date> was specified. Determine the start date by
stepping backwards one day at a time until we've reached a directory
containing a .cadc_ok file.

  $inst_start = $self->find_start_dates;


=item B<find_start_end_dates>

Returns a hash reference with instrument as keys & start date as
values, and an end date.

  ( $inst_start, $end ) = $copy->find_start_end_dates;


=item B<get_file_ids>

Given start and end date strings, returns an array reference of file
ids existing in database in the date range.

Throws JSA::Error::FatalError if there is a problem with database
query.

  $file_ids = $self->get_file_ids( '2008-08-20',
                                    '2008-08-21'
                                  );


=item B<instrument_ok_regex>

Returns the instrument specific regular expression if only the
instrument name is given.

  $regex = $copy->instrument_ok_regex( 'ACSIS' );

Sets the regular expression when regular expression is also given
along with instrument name; returns nothing.

  $copy->instrument_ok_regex( 'ACSIS', qr{^\.a\d{8}_(\d{5})\.ok$} );


=item B<instrument_path_regex>

Returns the instrument specific regular expression matching the path
for observation file(s) if only the instrument name is given.

  $regex = $copy->instrument_path_regex( 'ACSIS' );

Default for I<ACSIS> is ...

  qr{ ^
      # Parent instrument directory with date.
      ( .+?
        /
        \d{8}
      )
      [/\d]+?
      # Base file name.
      (
        ([ah])
        \d{8} _
        # Observation number.
        ( \d{5} )
        _ \d{2} _ \d{4} [.]sdf
      )
      $
    }x;


Sets the regular expression when regular expression is also given
along with instrument name; returns nothing.

  $copy->instrument_path_regex( 'ACSIS',
                                qr{^(/raw/path/\d{8})/.+?/(([.])[_\d]+\.sdf)$}
                              );


=item B<instrument_root>

Returns the instrument specific directory if only the instrument name
is given.

  $dir = $copy->instrument_root( 'ACSIS' );

Sets the instrument directory  when directory is also given along
with instrument name; returns nothing.

  $copy->instrument_root( 'ACSIS', '/path/to/jcmt/acsis/spectra' );


=item B<make_inst_date_path>

Returns the UT date directory path given the instrument name.

  $acsis_ut = $copy->make_inst_date_path( 'ACSIS', '20080820' );


=item B<make_obsnum_path>

Returns a observation directory name given a *.ok base name; the
parent directory; and a hash reference either with a regular
expression as the value & 'regex' as key, or instrument name as the
value & 'instrument' as the key (see I<instrument_ok_regex> method).

  # Returns '/parent/0123'.
  $obsnum_dir =
    $copy->make_obsnum_path( '.a_20080920_0123.ok',
                              '/parent',
                              { 'regex' => qr{ \d{8} _ (\d{4}) \.ok $}x }
                            );


=item B<okfiles>

Returns a hash reference to be manipulated by the calling method.  It
is used to pass around the same hash reference among the methods which
do not call each other.

  $files = $copy->okfiles;
  $files->{'ACSIS'}->{'2008-08-20 20:08:00'} = 'file';


=item B<start_date>

Returns the start date of data to be linked if no argument is given.

Else, sets the start date to the given argument, and returns nothing.
Date should either be a L<Time::Piece> object or a string in
C<YYYYMMDD> format.

  # Set a date.
  $copy->start_date( '20080820' );

  # Retrieve it.
  $date = $copy->start_date;


=item B<upload_per_instrument>

For each instrument, creates symbolic links to each data file from
start date to end date, going through each directory.
Later, creates a .cadc_ok file to indicate that the file should be
ignored in the future.

  $copy->upload_per_instrument;


=item B<upload_make_cadc_ok>

For every F<*.ok> file name, makes a symbolic link in CADC upload
directory and creates a corresponding F<*.cadc_ok> file (in the
directory name given by I<cadc_dir> method), given a hash of ...

=over 4

=item I<file-ids> as key

A array refernece of file ids as present in database as value.

=item I<instrument>

Instrument name involved.

If missing, then L<JSA::Error::BadArgs> exception is thrown.

=item I<obs-date>

UT date for an observation, used to generate source directory path.

If both this and I<source-dir> are missing, then
L<JSA::Error::BadArgs> exception is thrown.

=item I<okfiles>

Array reference of F<*.ok> file names.

=item I<source-dir>

Directory name where to find observation data for a given date &
observation number.

If missing, then I<obs-date> is used to generate the path.

=back

  $self->upload_make_cadc_ok( 'file-ids'   => \%file_id,
                              'instrument' => $inst,
                              'okfiles'   => $okfiles->{ $inst }->{ $ymd },
                              'source-dir' => '/insturment/date/path',
                            );



=item B<verbose>

Returns the verbosity value if no arguments are given.

  warn 'A warning' if $copy->verbose;

Sets the directory to the given argument, returns nothing.

  $copy->verbose( 2 );

=back

=head1 AUTHORS

Anubhav E<lt>a.agarwal@jach.hawaii.eduE<gt>,
Brad Cavanagh E<lt>b.cavanagh@jach.hawaii.eduE<gt>.

=head1 COPYRIGHT

Copyright (C) 2008,2009 Science and Technology Facilities Council.
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

