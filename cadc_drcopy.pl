#!perl

=head1 NAME

convert_dr_files - Convert files created by ORAC-DR into
CADC-compatible FITS files.

=head1 SYNOPSIS

  convert_dr_files

=head1 DESCRIPTION

This program is used to convert files created by ORAC-DR into FITS
files that can be injested by the CADC.

=head1 ARGUMENTS

=over 4

=item B<-date>

Specify the UT date of files to transfer, in YYYYMMDD format. Assumes
that the data will be located in
C</jcmtdata/reduced/INSTRUMENT/YYYYMMDD/>. Uses the current UT date if
not supplied.

=item B<-dir>

Specific directory for obtaining data products. Supercedes the
C<-date> argument.

=item B<-help>

Display a help message.

=item B<-man>

Display this man page.

=item B<-tempdir>

Override the temporary directory used for file conversion.

=item B<-transdir>

Override the CADC transfer directory. Defaults to the standard CADC
transfer directory (/jcmtdata/cadc/new).

=item B<-version>

Display the version number.

=back

=head1 ENVIRONMENT

This program requires that the $STARLINK_DIR environment variable is
correctly defined.

=head1 AUTHORS

Brad Cavanagh E<lt>b.cavanagh@jach.hawaii.eduE<gt>

=head1 COPYRIGHT

Copyright (C) 2008 Science and Technology Facilities Council.  All
Rights Reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3 of the License, or (at
your option) any later version.

This program is distributed in the hope that it will be useful,but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307,
USA.

=cut

use strict;
use warnings;

use File::Spec;
use Getopt::Long;
use Pod::Usage;

use JSA::Convert qw/ convert_dr_files /;
use JSA::Files qw/ scan_dir /;
use JSA::Headers qw/ read_headers /;

my %INSTRUMENTS = ( ACSIS => '/jcmtdata/reduced/acsis/',
                  );
my $CADCDIR = "/jcmtdata/cadc/new";

# Get command-line options.
my ( $date_str, $dir, $help, $man, $tempdir, $transdir, $version );
my $status = GetOptions( "date=s"     => \$date_str,
                         "dir=s"      => \$dir,
                         "help"       => \$help,
                         "man"        => \$man,
                         "tempdir=s"  => \$tempdir,
                         "transdir=s" => \$transdir,
                         "version"    => \$version,
                       );

pod2usage(1) if $help;
pod2usage( -exitstatus => 0, -verbose => 2 ) if $man;

if( $version ) {
  my $rev = '$Id: ';
  print "convert_dr_files - Convert ORAC-DR output to CADC-complaint files\n";
  print " Source code revision: $rev\n";
  exit;
}

# Get the current working directory.
my $curdir = File::Spec->rel2abs( File::Spec->curdir );

# Handle input and output directories.
my @INDIRS;
if( defined( $dir ) ) {
  $dir = File::Spec->rel2abs( $dir );
  log_message( "Using specified input directory '$dir'\n" );
  push @INDIRS, $dir;
} else {

  # We weren't given an input directory, so generate them from dates.
  my $ut;
  if( $date_str ) {
    if( $date_str =~ /^\d{8}$/ ) {
      $ut = $date_str;
    } else {
      die "Override date string must be in YYYYMMDD format\n";
    }
  } else {
    my @time = gmtime();
    $ut = sprintf("%04d%02d%02d", $time[5]+1900, $time[4]+1, $time[3]);
    print "Using current date: $ut\n";
  }

  @INDIRS = map { File::Spec->catdir( @_, $ut ) } values %INSTRUMENTS;

}
if( defined( $transdir ) ) {
  $transdir = File::Spec->rel2abs( $transdir );
  log_message( "Using specified output directory '$transdir'\n" );
} else {
  $transdir = $CADCDIR;
  log_message( "Using standard CADC directory for output '$transdir'\n" );
}

foreach my $indir ( @INDIRS ) {

  # Change to the input directory.
  chdir( $indir ) || die "Could not change to input directory '$indir'";

  # Scan for NDFs in the input directory and read their headers.
  my %files = scan_dir( qr/\.sdf$/ );
  my %drhdrs = read_headers( sort keys %files );

  # Convert the files.
  convert_dr_files( \%drhdrs, { indir => $indir,
                                outdir => $transdir,
                                tempdir => $tempdir,
                              } );

}

# Return to the original directory.
chdir( $curdir ) || die "Could not change to original directory '$curdir'";

exit;

sub log_message {
  my $message = shift;
  chomp( $message );
  print STDERR "$message\n";
}

