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
use warnings;
use File::Spec;

use Exporter 'import';
our @EXPORT_OK = qw( uri_to_file file_to_uri );

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

=back

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
