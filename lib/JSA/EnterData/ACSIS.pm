package JSA::EnterData::ACSIS;

use strict;
use warnings;

=head1 NAME

JSA::EnterData::ACSIS - ACSIS specific methods.

=head1 SYNOPSIS

  # Create new object, with specific header dictionary.
  my $inst = JSA::EnterData::ACSIS->new

  my $name = $inst->name;

  my @cmd = $inst->get_bound_check_command;
  system( @cmd ) == 0
    or die "Problem with running bound check command for $name.";

  # Use table in a SQL later.
  my $table = $inst->table;


=head1 DESCRIPTION

JAS::EnterData::ACSIS is a object oriented module, having instrument
specific methods in order to be called from L<JSA::EnterData>.

=head2 METHODS

=over 2

=cut

=item B<new>

Constructor, returns an I<JSA::EnterData::ACSIS> object.

  $enter = JSA::EnterData::ACSIS->new;

Currently, no extra arguments are handled.

=cut

sub new {

  my ( $class ) = @_;

  my $obj = '';
  return bless \$obj, $class;
}

=item B<get_bound_check_command>

Returns a list of command and its argument to be executed to
check/find the bounds.

  @cmd = $inst->get_bound_check_command;

  system( @cmd ) == 0
    or die "Problem running the bound check command";

=cut

sub get_bound_check_command {

  my ( $self, $fh, $pos_angle ) = @_;

  # Turn off autogrid; only rotate raster maps. Just need bounds.
  return
    ( '/star/bin/smurf/makecube',
      "in=^$fh",
      'system=ICRS',
      'out=!',
      'pixsize=1',
      # Do not care about POL.
      'polbinsize=!',
      # Turn off autogrid - only rotate raster maps. Just need bounds.
      'autogrid=no',
      'msg_filter=quiet',
      ( defined $pos_angle ? "crota=$pos_angle" : () ),
      'reset'
    );
}


=item B<name>

Returns the name of the instrument involved.

  $name = $inst->name;

=cut

sub name { return 'ACSIS' ; }


=item B<table>

Returns the database table related to the instrument.

  $table = $inst->table;

=cut

sub table { return 'ACSIS'; }


# Create obsid_subsysnr
sub _fill_headers_obsid_subsys {

  my ( $self, $header, $obsid ) = @_;

  # Create obsid_subsysnr
  $header->{'obsid_subsysnr'} = join '_', $obsid,  $header->{'SUBSYSNR'};

  $self->_print_text( sprintf "Created header [obsid_subsysnr] with value [%s]\n",
                        $header->{'obsid_subsysnr'}
                    )
    if $self->debug;

  return;
}


=item B<calc_freq>

Calculate frequency properties, updates given hash reference.

  JSA::EnterData->calc_freq( $obs, $headerref );

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


1;

=pod

=back

=head1 AUTHORS

=over 2

=item *

Anubhav E<lt>a.agarwal@jach.hawaii.eduE<gt>

=back

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

