package JSA::Headers::CADC;

=head1 NAME

JSA::Headers::CADC - CADC header functions

=head1 SYNOPSIS

  use JSA::Headers::CADC qw/ correct_asn_id /;
  $asn_id = correct_asn_id( $mode, $hdr, $usesurvey );

=head1 DESCRIPTION

This module provides helper functions that deal with
JSA headers mandated by the CADC.

=cut

use strict;
use warnings;
use warnings::register;

use Scalar::Util qw/ blessed /;

use JSA::Headers qw/ read_header /;

use Exporter 'import';
our @EXPORT_OK = qw/ correct_asn_id prepare_header_updates /;

=head1 FUNCTIONS

=over 4

=item B<correct_asn_id>

Given a FITS header containing an ASN_ID keyword and a processing
mode (night, obs or project) returns a version of the ASN_ID header
that will be unique for the combination of DR ASN_ID and processing
mode.

Returns undef if there is no ASN_ID header.

  $asn_id = correct_asn_id( $mode, $hdr, $usesurvey );

Processing mode can be one of ("obs", "night", "project", "public").
Nothing is prepended for "obs" processing. The UT date is prepended
in "night" mode, the SURVEY or project id is prepended in
project mode (although the survey is only used if the optional third
argument is true and the tile number is prepended in "public"
mode.

$hdr can be either an Astro::FITS::Header object or a reference to a hash.

=cut

sub correct_asn_id {
  my $mode = shift;
  my $hdr = shift;
  my $usesurvey = shift;

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

    # Remove components before "-" that would indicate that this ASN_ID
    # has been modified previously.  But take care of ASN_IDs which actually
    # include hyphens.  ASN_IDs known to do this are:
    #   * ACSIS "pub" products, e.g. 345796MHz-1000MHz-SSB
    # ASN_IDs not containing hyphens are:
    #   * Regular ASN_IDs consisting of md5sums
    #   * SCUBA-2 "pub" products, e.g. 450um
    while ($asn_id !~ /^\d{6}MHz/i) {
      last unless $asn_id =~ s/^[^-]*\-//;
    }

    my $prefix;
    if( $mode eq 'night' ) {
      $prefix = $header{UTDATE};
    } elsif( $mode eq 'project' ) {
      my $survey = $header{SURVEY};
      if( defined( $survey ) && $usesurvey ) {
        $prefix = $survey;
      } else {
        $prefix = $header{PROJECT};
      }
    } elsif ($mode eq 'public') {
      # Make sure we have a JSA HEALPix tile number available.
      my $tilenum = $header{'TILENUM'};
      die 'TILENUM header not defined for public product'
        unless defined $tilenum;

      # Instrument-specific behavior for "public" ASN_IDs.
      if ($header{'INSTRUME'} eq 'SCUBA-2') {
        # SCUBA-2 is a special case in that we actually want to throw
        # away the existing ASN_ID (which only contains filter wavelength)
        # so that the data from the two filters ends up in the same
        # "CompositeObservation" at CADC.  Therefore just return
        # the new string: instrument plus tile number.
        return sprintf('%s-%06d', 'SCUBA-2', $tilenum);
      }
      elsif ($header{'INSTRUME'} eq 'HARP' and $header{'BACKEND'} eq 'ACSIS') {
        $prefix = sprintf('%s-%06d', 'HARP-ACSIS', $tilenum);
      }
      else {
        die 'No public ASN_ID prefix defined for instrument ' .
            $header{'INSTRUME'} . ' and backend ' . $header{'BACKEND'};
      }
    }
    $asn_id = $prefix . '-' . $asn_id if defined $prefix;
  }
  return $asn_id;
}

=item B<prepare_header_updates>

Prepare a list of FITS header updates for a processed data file being
stored in the archive at CADC.

    @updates = @{prepare_header_updates($file, \%options)};

This function takes the name of the NDF file to be updated and
the following optional arguments: a hash reference with the
following allowed keys:

 - mode: Processing mode ("night", "project", "public")
 - dpdate: Date of processing in ISO8601 format
 - dpid  : Recipe instance ID associated with this processing
 - instream: Alternative INSTREAM header (default JCMT)

Returns a reference to an array of [header, value, comment] arrays.

=cut

sub prepare_header_updates {
  my $file = shift;
  my $options = shift;

  my @updates = ();

  my $mode = lc( $options->{'mode'} );
  my $instream = (exists $options->{'instream'} and
                  defined $options->{'instream'})
                      ? uc($options->{'instream'})
                      : 'JCMT';

  push @updates, ['INSTREAM', $instream, 'Source of input stream'];

  # Get the FITS headers.
  my $header = read_header( $file );

  # Retrieve the ASN_ID.
  my $asn_id = correct_asn_id( $mode, $header );

  if( defined( $asn_id ) ) {

    if( $mode eq 'project' ) {
      # Fix the ASN_TYPE header if we are a project and this is
      # an association (group coadd).
      push @updates, ['ASN_TYPE', 'project', 'Time-based selection criterion'];
    }
    elsif ($mode eq 'public') {
      push @updates, ['ASN_TYPE', 'public', 'Time-based selection criterion'];
    }

    # Write the ASN_ID header back into the FITS header.
    push @updates, ['ASN_ID', $asn_id, 'Association Identifier'];
  }

  if (exists $options->{dpdate} && defined $options->{dpdate}) {
    push @updates, ['DPDATE',  $options->{dpdate}, 'Data processing date'];
  }
  if (exists $options->{dpid} && defined $options->{dpid}) {
    push @updates, ['DPRCINST', $options->{dpid}, 'Data processing recipe instance ID'];
  }

  return \@updates;
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
