package JSA::CADC_Copy;

use warnings;
use strict;

#use File::Basename;
use File::Spec;
use Scalar::Util qw[ blessed looks_like_number ];
use Time::Piece;
use Time::Seconds qw[ ONE_DAY ];

use JSA::Error qw[ :try ];
use OMP::Config;
use OMP::DBbackend;

BEGIN {

  my %inst =
    (
      'ACSIS' =>
        {
          'ok-regex' => qr[^\. a \d{8} _ (\d{5}) \.ok$]x,
          'root' =>
            #'/jcmtdata/raw/acsis/spectra',
            '/home/agarwal/src/scicom/trunk/archiving/jcmt/.acsis/spectra',
        },

      'SCUBA2' =>
        {
          'ok-regex' => undef,
          'root' => undef,
        }
    );

  # Instruments to loop through
  my @inst = sort { lc $a cmp lc $b } keys %inst;

  sub instrument_list {

    my ( $class ) = @_;

    return @inst;
  }

  # Make instrument_root() & instrument_ok_regex() accessors with rudimentary
  # checking.
  for my $prop ( qw[ root ok-regex ] ) {

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

      # Indicator if to force upload.
      'force' => undef,

      'cadc-dir' =>
        #"/jcmtdata/cadc/new",
        '/home/agarwal/src/scicom/trunk/archiving/jcmt/.cadc/new/',
    ) ;

  # Make cadc_dir(), force(), verbose() accessors.
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

    # Hash of file id as key (undef as value).
    my $file_ids =
      $self
      ->get_file_ids(
                      $inst_start->strftime( $format ),
                      join ' ', $end->strftime( $format ), '23:59:59'
                    );

    unless ( defined $file_ids ) {

      if ( $self->verbose ) {

        printf "Nothing found between %s - %s for instrument %s.\n",
          $inst_start->strftime( $format ),
          join( ' ', $end->strftime( $format ), '23:59:59' ),
          $inst;
      }

      next;
    }

    my %file_id = map { $_->[0], undef } @{ $file_ids };

    my $ok_re = $self->instrument_ok_regex( $inst );

    while ( $inst_start->epoch <= $end->epoch ) {

      my $ymd = $inst_start->strftime( $format );
      my $src_dir = File::Spec->catdir( $self->instrument_root( $inst ), $ymd );

      if ( -e $src_dir ) {

        # Slurp up the .ok files if we didn't do so earlier while obtaining
        # the start date
        if ( ! exists $okfiles->{ $inst }->{ $ymd } ) {

          my $return = __PACKAGE__->read_okfiles( $src_dir );
          $okfiles->{ $inst }->{ $ymd } = $return->{'ok'};

        }

        $self->upload_make_cadc_ok( \%file_id,
                                    $ok_re,
                                    { 'ok' => $okfiles->{ $inst }->{ $ymd },
                                      'source-dir' => $src_dir,
                                    }
                                  );
      }

      $inst_start += ONE_DAY;
    }
  }

  return;
}

# Get list of files that have been uploaded to the db.
sub get_file_ids {

  my ( $self, $start, $end ) = @_;

  throw JSA::Error::BadArgs qq[Need start & end dates in "YYYYMMDD" format.\n]
    unless defined $start
    and    defined $end
    and $start =~ /^\d{8}$/
    and $end   =~ /^ \d{8} [ T] \d{2}:\d{2}:\d{2} $/x
    ;
    
  ( my $sql =
    q[ SELECT f.file_id
        FROM COMMON c, FILES f
        WHERE c.obsid = f.obsid
          AND c.date_obs > ?
          AND c.date_obs < ?
      ]
  ) =~ s/[ ]{2,}/ /g;

  # Connect to the CADC mirror DB
  my $back = OMP::DBbackend->new;
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

  return $dbh->selectall_arrayref( $sql, {}, $start, $end )
    or throw JSA::Error::FatalError
        sprintf "Could not perform DB query: %s\n", $dbh->errstr;
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
# do not have a corresponding .cadc_ok file.
sub upload_make_cadc_ok {

  my ( $self, $file_ids, $ok_re, $files ) = @_;

  for my $ok ( @{ $files->{'ok'} } ) {

    my ( $cadc_ok, $obsnum );
    for ( $ok ) {

      ( $obsnum ) = $_ =~ /$ok_re/;
      ( $cadc_ok = $_ ) =~ s/\.ok/\.cadc_ok/;
    }

    $cadc_ok = File::Spec->catfile( $files->{'source-dir'}, $cadc_ok );
    if ( -e $cadc_ok && ! $self->force ) {

      $self->verbose and print "$cadc_ok exists. Skipping.\n";

      next;
    }

    $obsnum
      or throw JSA::Error::FatalError "File name [$ok] does not match regexp.";

    my $obs = File::Spec->catdir( $files->{'source-dir'}, $obsnum );

    my @files = @{ __PACKAGE__->filter_files( $obs, $file_ids ) };

    for my $base ( @files ) {

      $self->make_symlink( $obs, $self->cadc_dir, $base );
    }

    # XXX - Need to touch .cadc_ok in the sub directory containing the files,
    # instead of the parent directory.
    $self->make_empty_file( $cadc_ok ) if scalar @files;
  }

  return;
}

# Returns the files in the given directory that have been replicated to the CADC
# mirror db specified by the hash reference.
sub filter_files {

  my ( $class, $dir, $replicated ) = @_;

  opendir my $dh, $dir
    or throw JSA::Error::FatalError
        sprintf "Could not open directory [%s]: %s\n", $dir, $!;

  my @files = grep { exists $replicated->{ $_ } } readdir $dh;

  closedir $dh;

  return \@files;
}

# Makes a symbolic link for given base file name & source directory in the
# destination directory.
sub make_symlink {

  my ( $self, $src, $dest, $base ) = @_;

  my $path = File::Spec->catfile( $src, $base );
  my $link = File::Spec->catfile( $dest, $base );

  symlink $path, $link
    or throw JSA::Error::FatalError
        sprintf "Can't create symlink to %s in directory %s: %s.",
          $path, $dest, $!;

  $self->verbose and print "Created link: $path -> $link\n";

  return;
}

# Creates an empty file at the given path.
sub make_empty_file {

  my ( $self, $path ) = @_;

  return if -e $path;

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
      my $src_dir = File::Spec->catdir( $self->instrument_root( $inst ), $ymd );

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


=item B<make_empty_file>

Given a file path, creates an empty file.  Nothing is done if the path
already exists.

Throws JSA::Error::FatalError if the file cannot be created.

  $copy->make_empty_file( '/file/path' );


=item B<make_symlink>

Given parent observation directory, parent directory for CADC files,
and base file name, creates the link from file with the base name in
observation directory to file in CADC directory.

Throws JSA::Error::FatalError if the symbolic link cannot be created.

  $copy->make_symlink( '/obs/dir', '/cadc/dir', 'a-123.sdf' );


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

Sets the regular expression  when regular expression is also given
along with instrument name; returns nothing.

  $copy->instrument_ok_regex( 'ACSIS', qr{^\.a\d{8}_(\d{5})\.ok$} );


=item B<instrument_root>

Returns the instrument specific directory if only the instrument name
is given.

  $dir = $copy->instrument_root( 'ACSIS' );

Sets the instrument directory  when directory is also given along
with instrument name; returns nothing.

  $copy->instrument_root( 'ACSIS', '/path/to/jcmt/acsis/spectra' );


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
directory name given by I<cadc_dir> method), given ...

=over 4

=item *

a hash reference with file ids to be uploaded as keys;

=item *

a regular expression to extract observation number from every F<*.ok>
file name;

=item *

a hash with C<ok> & C<source-dir> as keys and array reference of
F<*.ok> file names & source directory name as respective values.

=back

  $copy->upload_make_cadc_ok( \%file_id,
                              $obsnum_regex,
                              {
                                'ok' => \@okfiles,
                                'source-dir' => '/instrument/date/src'
                              }
                            );


=item B<verbose>

Returns the verbosity value if no arguments are given.

  warn 'A warning' if $copy->verbose;

Sets the directory to the given argument, returns nothing.

  $copy->verbose( 2 );


=back

=head1 AUTHOR

Anubhav E<lt>a.agarwal@jach.hawaii.eduE<gt>

=head1 COPYRIGHT

Copyright (C) 2008, Science and Technology Facilities Council.
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

