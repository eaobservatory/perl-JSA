package JSA::QA::Result;

use 5.006;
use strict;
use warnings;
use Carp;

use vars qw/ $VERSION $DEBUG /;

use Exporter 'import';

sub new {
  my $proto = shift;
  my $class = ref( $proto ) || $proto;

  my $result = bless { 'PASS' => 1,
                       'BAD_RECEPTORS' => [],
                       'FAIL_REASONS' => [],
                       'RMS_STATS' => (),
                       'TSYS_STATS' => (),
                     }, $class;

  $result->_configure( @_ ) if @_;

  return $result;
}

sub bad_receptors {
  my $self = shift;
  if( @_ ) { $self->{BAD_RECEPTORS} = shift; }
  return $self->{BAD_RECEPTORS};
}

sub fail_reasons {
  my $self = shift;
  if( @_ ) { $self->{FAIL_REASONS} = shift; }
  return $self->{FAIL_REASONS};
}

sub pass {
  my $self = shift;
  if( @_ ) { $self->{PASS} = shift; }
  return $self->{PASS};
}

sub rms_stats {
  my $self = shift;
  if( @_ ) { $self->{RMS_STATS} = shift; }
  return $self->{RMS_STATS};
}

sub tsys_stats {
  my $self = shift;
  if( @_ ) { $self->{TSYS_STATS} = shift; }
  return $self->{TSYS_STATS};
}

sub add_bad_receptor {
  my $self = shift;
  my $receptor = shift;

  return if ! defined $receptor;

  my $bad_receptors = $self->bad_receptors;
  if( ref( $receptor ) ) {
    push @$bad_receptors, @$receptor;
  } else {
    push @$bad_receptors, $receptor;
  }
  $self->bad_receptors( $bad_receptors );
}

sub add_fail_reason {
  my $self = shift;
  my $reason = shift;

  return if ! defined $reason;

  my $reasons = $self->fail_reasons;
  if( ref( $reason ) ) {
    push @$reasons, @$reason;
  } else {
    push @$reasons, $reason;
  }
  $self->fail_reasons( $reasons );
}

=item B<merge>

Merge two sets of QA results.

  $merged = $first->merge( $second );

This method only merges the pass(), bad_receptors(), and
fail_reasons() values.

Returns a JSA::QA::Result object. The input JSA::QA::Result objects
are unchanged.

=cut

sub merge {
  my $self = shift;
  my $second = shift;

  my $merged = new JSA::QA::Result;
  $merged->pass( $self->pass && $second->pass );

  $merged->add_fail_reason( $self->fail_reasons );
  $merged->add_fail_reason( $second->fail_reasons );

  $merged->add_bad_receptor( $self->bad_receptors );
  $merged->add_bad_receptor( $second->bad_receptors );

  return $merged;
}

sub _configure {
  my $self = shift;
  my %args = @_;

  foreach my $key ( keys %args ) {
    if( $self->can( $key ) ) {
      $self->$key( $args{$key} );
    }
  }
}

1;
