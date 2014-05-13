package JSA::EnterData::Assoc;

use strict; use warnings;

our $VERSION = '0.01';

use Exporter 'import';
@EXPORT_OK = qw( get_asn_id );

use Log::Log4perl;

use JSA::Error       qw[ :try ];
use JSA::Headers     (); #get_orac_instrument
use ORAC::Inst::Defn (); #orac_determine_inst_classes

my %loaded;

sub get_asn_id {

  my ( $obs ) = @_;

  my $orac_inst = JSA::Headers::get_orac_instrument( $obs->fits() );
  my ( $frame_class, $group, $cal, $inst ) =
    ORAC::Inst::Defn::orac_determine_inst_classes( $orac_inst );

  my $log = Log::Log4perl->get_logger( '' );

  unless ( exists $loaded{ $frame_class } ) {

    ( my $load = $frame_class ) =~ s{::}{/}g;
    $load .= q[.pm];
    eval { require $load; 1; }
      or $log->logcroak( "Could not load '$frame_class' class: ", $@  );
  }

  my $headers = $obs->hdrhash();

  $ENV{'ORAC_INSTRUMENT'} =
    exists $headers->{'SUBSYSNR'} && defined $headers->{'SUBSYSNR'}
    && JSA::EnterData::SCUBA2->name_is_scuba2( $obs->backend() )
    ? 'SCUBA2_' . $headers->{'SUBSYSNR'}
    : ''
    ;

  my %out;
  for my $subsys ( $obs->subsystems() ) {

    my @idss = $subsys->obsidss();
    my $idss = $idss[0];
    unless ( $idss ) {

      $log->warn( 'Cannot find OBDISS for an observation.' );
      next;
    }

    my $frame = $frame_class->new();
    $frame->configure( [ $subsys->filename() ] );

    my $id =  $frame->jsa_pub_asn_id() or next;
    push @{ $out{ $idss } }, $id;
  }
  return %out;
}

1;

__END__

=pod

=head1 NAME

JSA::EnterData::Assoc - Find asn_id.

=head1 SYNOPSIS

To get ASN ID for ACSIS(HARP) or SCUBA2 instrument ...

  use  JSA::EnterData::Assoc;
  $asn_list = JSA::EnterData::Assoc::get_asn_id( $obs );

=head1  DESCRIPTION

Currently it is a function based module. Nothing is exported by default.

=head2 Functions

=over 2

=item B<get_asn_id>

Given a L<OMP::Info::Obs> object, returns a hash of C<obsid_subsysnr> as keys
asn ASN ID as array reference values. It can be imported in your code.

 $list = get_asn_id( $obs );

=back

=head2 See Also

=over 2

=item L<OMP::Info::Obs>

=item I<ORAC-DR> Perl modules

=back

=head1 COPYRIGHT, LICENSE

Copyright (C) 2014, Science and Technology Facilities Council.
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

