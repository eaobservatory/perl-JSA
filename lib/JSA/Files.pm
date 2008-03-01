package JSA::Files;

=head1 NAME

JSA::Files - File naming and URIs in the JCMT Science Archive

=head1 SYNOPSIS

  use JSA::Files qw/ uri_to_file file_to_uri /;

  $file = uri_to_file( $uri );
  $uri = file_to_uri( $file );

=head1 DESCRIPTION

Helper routines for generating file names that are JSA compliant or
for converting URIs into filenames.

=cut

use strict;
use Carp;
use warnings;
use File::Spec;
use warnings::register;

use JSA::Error;

use Exporter 'import';
our @EXPORT_OK = qw( uri_to_file file_to_uri drfilename_to_cadc
                     cadc_to_drfilename looks_like_drfile looks_like_cadcfile
                     compare_file_lists scan_dir );


# List of support product types as found in ASN_TYPE FITS header
# along with corresponding names found in filenames.
my %PRODUCT_TYPES = (
	     night => 'nit',
	     public => 'pub',
	     obs => 'obs',
	     project => 'pro',
);

# invert it
my %FILE_ABBREV_TO_PROD_TYPE;
while ( my ($k,$v) = each(%PRODUCT_TYPES) ) {
  $FILE_ABBREV_TO_PROD_TYPE{$v} = $k;
}


=head1 FUNCTIONS

=over 4

=item B<uri_to_file>

Given a URI of the form ad:JCMT/xxxx convert it to a filename.

  $file = uri_to_file( $uri );

The returned file name does not include a path.
Returns undef if the URI is not recognized.

=cut

sub uri_to_file {
  my $uri = shift;
  my $file;
  if ($uri =~ /^ad:JCMT\//) {
    # chop off the front
    $file = $uri;
    $file =~ s/^ad:JCMT\///;
    # append the suffix
    $file .= ".fits";
  }
  return $file;
}

=item B<file_to_uri>

Given a file name, remove any path and file suffix and
convert into a URI.

 $uri = file_to_uri( $file );

No check is made for allowed file suffices.

=cut

sub file_to_uri {
  my $path = shift;
  my ($vol, $dir, $file) = File::Spec->splitpath( $path );
  my $uri;
  if ($file) {
    # strip suffix
    $file =~ s/\.[a-zA-Z]+$//;
    $uri = "ad:JCMT/". $file;
  }
  return $uri;
}

=item B<looks_like_drfile>

Examines the supplied filename to determine whether it looks like
a data file produced by the DR pipeline.

  $isdr = looks_like_drfile( $filename );

The file suffix must be ".sdf".

Directory information is stripped prior to the check.

=cut

sub looks_like_drfile {
  my $filename = shift;
  $filename = _strip_path( $filename );

  # The pattern matches are not full proof if a UT date has the year
  # 3000 in it for example

  # do not check that the "a" corresponds to the correct UT date for
  # DAS -> ACSIS transition
  
  if ($filename =~ /^g?[ah]\d{8}_\d{5}_\d\d_\w+\.sdf$/) {
    # ACSIS
    return 1;
  } elsif ($filename =~ /^g?s\d{8}_\d{5}_\d{3}_\w+\.sdf$/) {
    # SCUBA-2
    return 1;
  }
  return 0;
}

=item B<looks_like_cadcfile>

See if the supplied file looks like it uses the CADC naming convention.

  $iscadc = looks_like_cadcfile( $file );

The file suffix must be ".fits". Directory information is stripped
before testing the file for compliance.

=cut

sub looks_like_cadcfile {
  my $filename = shift;
  $filename = _strip_path( $filename );

  # These pattern matches are not bulletproof
  if ($filename =~ /^jcmth\d{8}_\d{5}_\d{2}_\w+_[a-z]{3}_\d{3}\.fits/) {
    return 1; # Heterodyne
  } elsif ($filename =~ /^jcmts\d{8}_\d{5}_\d{3}_\w+_[a-z]{3}_\d{3}\.fits/) {
    return 1; # SCUBA-2
  }
  return 0;
}


=item B<dissect_drfile>

Split a filename generated by the pipeline into its component parts.

 ($isgroup, $prefix, $utdate, $obsnum, $subsys, $product, $prodcount)
    = dissect_drfile( $drfile );

The group flag is a boolean. The observation number, subsystem number
(wavelength for SCUBA-2) and product count will not be zero-padded.
The product count can be undefined.

Returns empty list if the supplied file does not look like a pipeline
product.

=cut

sub dissect_drfile {
  my $drfile = shift;
  my $original = $drfile;
  return () unless looks_like_drfile( $drfile );
  $drfile = _strip_path( $drfile );

  my $isgroup = ($drfile =~ s/^g//);
  my ($prefix, $utdate,$obsnum,$subsys,$product,$prodcount);
  if ($drfile =~ /^([ahs])(\d{8})_(\d{5})_(\d{2,3})_([a-z]+)(\d*)\.sdf$/) {
    $prefix = $1;
    $utdate = $2;
    $obsnum = $3;
    $subsys = $4;
    $product= $5;
    $prodcount = $6;
    $prodcount = undef if (defined $prodcount && $prodcount eq '');

    # Clean up strings
    $obsnum =~ s/^0+//;
    $subsys =~ s/^0+//;
    $prodcount =~ s/^0+// if defined $prodcount;

    return ($isgroup, $prefix,$utdate,$obsnum,$subsys,$product,$prodcount);
  } else {
    JSA::Error::FatalError->throw( "DR file '$original' looked okay but failed pattern match" );
  }
  return ();
}

=item B<dissect_cadcfile>

Given a CADC filename, split it into its component parts.

 @parts = dissect_cadcfile( $cadcfile );

Where the parts are defined as

  telescope - "jcmt"
  prefix
  utdate
  obsnum
  subsys
  product
  prodcount
  type
  version

The .fits suffix is not included in the returned list but should be present
in the supplied filename.

=cut

sub dissect_cadcfile {
  my $cadcfile = shift;
  return () unless looks_like_cadcfile( $cadcfile );
  $cadcfile = _strip_path( $cadcfile );

  my ($prefix, $utdate, $obsnum, $subsys, $product, $prodcount,
      $type, $version);
  if ($cadcfile =~ /^jcmt([hs])(\d{8})_(\d{5})_(\d{2,3})_(\w+)_([a-z]{3})_(\d{3})\.fits/) {
    $prefix = $1;
    $utdate = $2;
    $obsnum = $3;
    $subsys = $4;
    $product= $5;
    $type = $6;
    $version = $7;

    # split product into product and number
    if ($product =~ /([a-z]+)(\d{3})/) {
      $product = $1;
      $prodcount = $2;
    }

    # Clean up strings
    $obsnum =~ s/^0+//;
    $subsys =~ s/^0+//;
    $prodcount =~ s/^0+// if defined $prodcount;
    $version =~ s/^0+//;

    return ($prefix,$utdate,$obsnum,$subsys,$product,$prodcount,$type,$version);
  } else {
    JSA::Error::FatalError->throw("CADC file '$cadcfile' looked okay but failed pattern match");
  } 
  return ();
}

=item B<drfilename_to_cadc>

Convert a pipeline output filename into standard CADC file naming
convention.

  $cadcname = drfilename_to_cadc( $drname );

Association type and version number can be supplied using hash syntax.
Version number will default to 0 and in almost all cases that is the
correct value. Association type is mandatory for all group files.

  $cadcname = drfilename_to_cadc( $drname, ASN_TYPE => $type,
                                           VERSION => 0 );

The type describes the association to be used for this file. Options
are 'obs', 'night', 'project', 'public' or the abbreviated translated
forms. The type is optional for an 'obs' file since the type can be determined
from the filename.

If the input file does not look like it is in the correct format,
an undefined value is returned.

The name conversion is attempted even if the supplied name does not
look like a DR product. The file is not opened to check that there
are PRODUCT and ASN_TYPE headers.

If a directory path is included in the supplied name it will
be included in the returned name.

=cut

sub drfilename_to_cadc {
  my $drname = shift;
  my %defaults = ( ASN_TYPE => undef, VERSION => 0 );
  my %args = (%defaults, @_);

  # Get the directory name
  my ($dir, $filepart) = _strip_path( $drname );
  
  # Split file into components. If this returns empty then we know
  # it did not look like a drfile so no need to call looks_like_drfile
  my ($isgroup, $prefix, $utdate, $obsnum, $subsys, $product, $prodcount)
    = dissect_drfile( $drname );
  return () if !defined $utdate;

  my $type = $args{ASN_TYPE};
  if ($isgroup) {
    # we will need a type
    if (!defined $type) {
      JSA::Error::BadArgs->throw( "Must supply a association type for group products" );
    } elsif (exists $PRODUCT_TYPES{$type}) {
      $type = $PRODUCT_TYPES{$type};
    } elsif (exists $FILE_ABBREV_TO_PROD_TYPE{$type}) {
      # type is okay
    } else {
      JSA::Error::BadArgs->throw( "drfilename_to_cadc: Unrecognized association type '$type' given" );
    }

    # Should not get a type of "obs"
    if ($type eq 'obs') {
      JSA::Error::BadArgs->throw( "This is a group observation but it is tagged as an 'obs' product" );
    }

  } else {
    # Always force to 'obs' but warn if it was something different
    if (defined $type && $type ne 'obs') {
      warnings::warnif( "This file looks like an 'obs' product but is tagged with something else. Forcing to 'obs'.");
    }
    $type = 'obs';
  }

  # Prefix of "a" is now meant to be "h"
  $prefix = "h" if $prefix eq 'a';

  # _cube has a mandatory count and some earlier pipeline versions
  # did not support that. This check is probably obsolete
  if ($product eq 'cube' && !defined $prodcount) {
    $prodcount = 1;
  }

  # The product count is only formatted if defined
  my $p = ( defined $prodcount ? "%03d" : "" );

  # Note that we format subsystem as %02d because this will work
  # for SCUBA-2 850/450 without breaking ACSIS 2digit.

  # Now form the new filename
  my $new = sprintf('%s%s%08d_%05d_%02d_%s'.$p.'_%s_%03d.%s',
                    "jcmt", $prefix, $utdate, $obsnum, $subsys,
                    $product, (defined $prodcount ? $prodcount : () ),
                    $type, $args{VERSION}, "fits");

  # prepend directory if needed
  if ($dir) {
    $new = File::Spec->catfile( $dir, $new );
  }
  return $new;
}

=item B<cadc_to_drfilename>

Convert a CADC formatted filename to the original DR filename.

  $drname = cadc_to_drfilename( $cadcname );

Note that the 'h'->'a' replacement in prefix is dependent on
the content of the YYYYMMMDD ut date.

Undef is returned if the file does not look like a CADC filename .

If a directory path is included in the supplied name it will
be included in the returned name.

=cut

sub cadc_to_drfilename {
  my $cadcfile = shift;

  # Get the directory name
  my ($dir, $filepart) = _strip_path( $cadcfile );

  # split into parts. Will fail if name does not look like CADC name
  my @parts = dissect_cadcfile( $cadcfile );
  return () unless @parts;
  return unless looks_like_cadcfile( $cadcfile );

  # Sort out acsis prefix
  if ($parts[0] eq 'h' && $parts[1] > 20060801) {
    $parts[0] = 'a';
  }

  # product formatting
  my $p = ( defined $parts[5] ? "%03d" : "" );

  my $new = sprintf( '%s%08d_%05d_%02d_%s'.$p.'.sdf',
                     @parts[0..4],(defined $parts[5] ? $parts[5] : () ) );

  # prepend directory if needed
  if ($dir) {
    $new = File::Spec->catfile( $dir, $new );
  }
  return $new;
}

=item B<scan_dir>

Scan the current directory looking for files matching a particular
pattern. It runs stat() files or lstat() on links and return the output of stat
as a reference to an array in a hash indexed by filename. The hash is
suitably configured to be used by compare_file_lists().

  %files = scan_dir( qr/\.(sdf|fits)$/ );

If no pattern is supplied the default is to scan for FITs and
SDF suffix filenames.

 %files = scan_dir();

=cut

sub scan_dir {
  my $pattern = shift;
  $pattern = qr/\.(sdf|fits)$/ unless defined $pattern;
  
  opendir(my $dh, File::Spec->curdir)
    or croak "Could not open data directory to scan it: $!";

  my %files;
  while (defined( my $file = readdir($dh) ) ) {

    if ($file =~ $pattern) {
      if (-l $file) {
        $files{$file} = [ lstat($file) ];
      } else {
        $files{$file} = [ stat($file) ];
      }
    }
  }
  
  closedir($dh) or croak "Could not close data directory after scan: $!";
  return %files;
}

=item B<compare_file_lists>

Compare the scan done by C<scan_dir> before with the scan after and return anything that
is newer than before or is not present in the old scan.

  @new = compare_file_lists(\%old, \%new);

=cut

sub compare_file_lists {
  my $original = shift;
  my $after = shift;

  my @files;
  for my $current (keys %$after) {
    if (!exists $original->{$current}) {
      # must be a new file since it did not exist before
      push(@files, $current);
    } elsif ( $original->{$current}->[9] < $after->{$current}->[9]) {
      # modified since the original scan
      push(@files, $current);
    }
  }
  return @files;
}

=back

=begin PRIVATE

=head1 INTERNAL ROUTINES

=over 4

=item B<_strip_path>

Given a filename that may include a directory path, split the
name into the path and the base filename.

In scalar context returns just the base filename. In list context
returns the directory and file information.

  ($dir, $file) = _strip_path( $path );
  $file  = _strip_path( $path );

=cut

sub _strip_path {
  my $path = shift;

  my ($vol, $dir, $file) = File::Spec->splitpath( $path );
  if (wantarray()) {
    return ($dir, $file);
  } else {
    return $file;
  }
}

=back

=end PRIVATE

=head1 AUTHORS

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>,

=head1 COPYRIGHT

Copyright (C) 2008 Science and Technology Facilities Council.
All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful,but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place,Suite 330, Boston, MA  02111-1307, USA

=cut

1;
