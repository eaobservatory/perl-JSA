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
                       'NOTES' => [],
                       'RMS_STATS' => {},
                       'TSYS_STATS' => {},
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

sub notes {
  my $self = shift;
  if( @_ ) { $self->{NOTES} = shift; }
  return $self->{NOTES};
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

sub clear_fail_reasons {
  my $self = shift;
  $self->{FAIL_REASONS} = [];
}

sub add_note {
  my $self = shift;
  my $note = shift;

  return if ! defined $note;

  my $notes = $self->notes;
  if( ref( $note ) ) {
    push @$notes, @$note;
  } else {
    push @$notes, $note;
  }
  $self->notes( $notes );
}

=item B<merge>

Merge two sets of QA results.

  $merged = $first->merge( $second );

This method only merges the bad_receptors(), notes() and
fail_reasons() values. pass() is determined by AND'ing the pass()
values for the two sets of QA results. rms_stats() and tsys_stats()
are taken from the first set of QA results, unless the respective
value is undefined and the value from the second set is defined.

Returns a JSA::QA::Result object. The input JSA::QA::Result objects
are unchanged.

=cut

sub merge {
  my $first = shift;
  my $second = shift;

  my $merged = new JSA::QA::Result;
  $merged->pass( $first->pass && $second->pass );

  $merged->add_fail_reason( $first->fail_reasons );
  $merged->add_fail_reason( $second->fail_reasons );

  $merged->add_bad_receptor( $first->bad_receptors );
  $merged->add_bad_receptor( $second->bad_receptors );

  $merged->add_note( $first->notes );
  $merged->add_note( $second->notes );

  if( ( scalar keys %{$first->rms_stats} ) > 1 ) {
    $merged->rms_stats( $first->rms_stats );
  } elsif( ( scalar keys %{$second->rms_stats} ) > 1 ) {
    $merged->rms_stats( $second->rms_stats );
  }

  if( ( scalar keys %{$first->tsys_stats} ) > 1 ) {
    $merged->tsys_stats( $first->tsys_stats );
  } elsif( ( scalar keys %{$second->tsys_stats} ) > 1 ) {
    $merged->tsys_stats( $second->tsys_stats );
  }

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
