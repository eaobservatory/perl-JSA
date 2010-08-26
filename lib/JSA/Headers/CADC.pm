package JSA::Headers::CADC;

=head1 NAME

JSA::Headers::CADC - CADC header functions

=head1 SYNOPSIS

  use JSA::Headers::CADC qw/ correct_asn_id /;
  $asn_id = correct_asn_id( $mode, $hdr );

=head1 DESCRIPTION

This module provides helper functions that deal with
JSA headers mandated by the CADC.

=cut

use strict;
use warnings;
use warnings::register;

use Scalar::Util qw/ blessed /;

use Exporter 'import';
our @EXPORT_OK = qw/ correct_asn_id /;

=head1 FUNCTIONS

=over 4

=item B<correct_asn_id>

Given a FITS header containing an ASN_ID keyword and a processing
mode (night, obs or project) returns a version of the ASN_ID header
that will be unique for the combination of DR ASN_ID and processing
mode.

Returns undef if there is no ASN_ID header.

  $asn_id = correct_asn_id( $mode, $hdr );

Processing mode can be one of ("obs", "night", "project", "public").
Nothing is prepended for "obs" or "public" processing. The UT date is prepended
in "night" mode and the SURVEY or project id is prepended in
project mode.

$hdr can be either an Astro::FITS::Header object or a reference to a hash.

=cut

sub correct_asn_id {
  my $mode = shift;
  my $hdr = shift;

  my %header;
  if (blessed($hdr)) {
    tie %header, "Astro::FITS::Header", $hdr, tiereturnsref => 1;
  } else {
    %header = %$hdr;
  }

  my $asn_id = $header{ASN_ID};

  # Depending on the mode, append the value of a specific header, but
  # only if ASN_ID is defined.
  if( defined( $asn_id ) ) {

    # Remove anything before the "-" that would indicate that this ASN_ID
    # has been modified previously
    $asn_id =~ s/.*\-//;

    my $prefix;
    if( $mode eq 'night' ) {
      $prefix = $header{UTDATE};
    } elsif( $mode eq 'project' ) {
      my $survey = $header{SURVEY};
      if( defined( $survey ) ) {
        $prefix = $survey;
      } else {
        $prefix = $header{PROJECT};
      }
    }
    $asn_id = $prefix . '-' . $asn_id if defined $prefix;
  }
  return $asn_id;
}


=back

=head1  AUTHORS

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>,
Brad Cavanagh E<lt>b.cavanagh@jach.hawaii.eduE<gt>,

=head1 COPYRIGHT

Copyright (C) 2009-2010 Science and Technology Facilities Council.
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