package JSA::Headers::Starlink;

=head1 NAME

JSA::Headers::Starlink - Helper functions to deal with file headers
using Starlink tasks.

=head1 SYNOPSIS

  use JSA::Headers::Starlink;
  update_fits_headers( $file );
  add_fits_comments( $file, \@comments );

=head1 DESCRIPTION

This module provides helper functions that handle file headers for
NDFs.

=cut

use strict;
use warnings;
use warnings::register;

use File::Spec;
use File::Temp;

use JSA::Starlink qw/ check_star_env run_star_command /;
use JSA::Headers qw/ read_header /;
use JSA::Headers::CADC qw/ correct_asn_id /;

use Exporter 'import';
our @EXPORT_OK = qw/ update_fits_headers add_fits_comments /;

=head1 FUNCTIONS

=over 4

=item B<update_fits_headers>

Update CADC-specific FITS headers in an NDF file.

  update_fits_headers( $file, \%options );

This function updates one FITS header:

 o INSTREAM: set to 'JCMT'

This function takes one mandatory argument: the NDF file to be
updated.

This function takes the following optional arguments: a hash reference with the
following allowed keys:

 - mode: Processing mode ("night", "project", "public")
 - dpdate: Date of processing in ISO8601 format
 - dpid  : Recipe instance ID associated with this processing
 - instream: Alternative INSTREAM header (default JCMT)
 - fitsmod_extra: List of extra commands to give to FITSMOD
                  as if entered with MODE=FILE

This function does not return anything.

=cut

sub update_fits_headers {
  my $file = shift;

  my $options = shift;
  my $mode = lc( $options->{'mode'} );
  my $instream = (exists $options->{'instream'} and
                  defined $options->{'instream'})
                      ? uc($options->{'instream'})
                      : 'JCMT';

  check_star_env( "KAPPA", "fitsmod" );

  # We will always be writing one header but with the possibility
  # of more. For efficiency we only want to call FITSMOD once
  # so we create a command file
  my $tmpfile = File::Temp->new();
  print $tmpfile "A INSTREAM $instream Source of input stream\n";

  # Get the FITS headers.
  my $header = read_header( $file );

  # Retrieve the ASN_ID.
  my $asn_id = correct_asn_id( $mode, $header );

  if( defined( $asn_id ) ) {

    if( $mode eq 'project' ) {
      # Fix the ASN_TYPE header if we are a project and this is
      # an association (group coadd).
      print $tmpfile "A ASN_TYPE project \$C\n";
    }

    # Write the ASN_ID header back into the FITS header.
    print $tmpfile "A ASN_ID $asn_id \$C\n";
  }

  if (exists $options->{dpdate} && defined $options->{dpdate}) {
    print $tmpfile "A DPDATE ".$options->{dpdate}." Data processing date\n";
  }
  if (exists $options->{dpid} && defined $options->{dpid}) {
    print $tmpfile "A DPRCINST ".$options->{dpid}." Data processing recipe instance ID\n";
  }
  if (exists $options->{'fitsmod_extra'}
      and defined $options->{'fitsmod_extra'}) {
    print $tmpfile $_ . "\n" foreach @{$options->{'fitsmod_extra'}};
  }

  close($tmpfile);
  my @args = ( File::Spec->catfile( $ENV{KAPPA_DIR}, "fitsmod"),
               "NDF=$file",
               "MODE=File",
               "TABLE=$tmpfile"
             );

  run_star_command( @args );
}

=item B<add_fits_comments>

Add the given text as a COMMENT block in the fits header of the
supplied file.

 add_fits_comments( $file, \@comments );

Currently uses fitsmod rather than Astro::FITS::Header.

=cut

sub add_fits_comments {
  my $file = shift;
  my $com = shift;

  check_star_env( "KAPPA", "fitsmod" );

  # Fitsmod requires a text file
  my $tmp = File::Temp->new();
  for my $c (@$com) {
    my $line = $c;
    # escape leading "
    $line =~ s/^\"/\"\"/;
    print $tmp "W COMMENT . $line\n";
  }
  close($tmp);

  my @args = ( File::Spec->catfile( $ENV{KAPPA_DIR}, "fitsmod"),
               "NDF=$file",
               "MODE=FILE",
               "TABLE=$tmp" );

  run_star_command( @args );
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
