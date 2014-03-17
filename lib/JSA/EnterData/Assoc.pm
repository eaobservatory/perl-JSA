package JSA::EnterData::Assoc;

use strict; use warnings FATAL => 'all';

our $VERSION = '0.00';

use JSA::Error       qw[ :try ];
use JSA::Headers     (); #get_orac_instrument
use ORAC::Inst::Defn (); #orac_determine_inst_classes

my %loaded;

sub get_asn_id {

  my ( $class, $obs ) = @_;

  my $orac_inst = JSA::Headers::get_orac_instrument( $obs->fits() );
  my ( $frame_class, $group, $cal, $inst ) =
    ORAC::Inst::Defn::orac_determine_inst_classes( $orac_inst );

  unless ( exists $loaded{ $frame_class } ) {

    ( my $load = $frame_class ) =~ s{::}{/}g;
    $load .= q[.pm];
    eval { require $load; 1; }
      or die "Could not load '$frame_class' class: ", $@ ;
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

      Carp::carp( 'Cannot find OBDISS for an observation.' );
      next;
    }

    my $frame = $frame_class->new();
    $frame->configure( [ $subsys->filename() ] );
    $frame->findgroup();

    push @{ $out{ $idss } } , $frame->asn_id();
  }

  return %out;
}

1;

__END__

=pod

=head1 NAME

JSA::EnterData::Assoc - Find asn_id.

=head1 SYNOPSIS


=head1  DESCRIPTION



=head2  METHODS|FUNCTIONS

=over 2

=item *


=item *


=back

=cut


