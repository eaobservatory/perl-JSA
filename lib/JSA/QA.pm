package JSA::QA;

=head1 NAME

JSA::QA - Quality Assurance functions for ACSIS data.

=head1 SYNOPSIS

  use JSA::QA qw/ analyse_tsys /;
  analyse_tsys( $tsys );

=head1 DESCRIPTION

This module provides functions that analyse various statistics gleaned
from JCMT ACSIS data. In doing so, it provides data suitability
statistics for the various JCMT Legacy Surveys.

=cut

use 5.006;
use strict;
use warnings;
use warnings::register;
use Carp;

use vars qw/ $VERSION $DEBUG /;

use Exporter 'import';
our @EXPORT_OK = qw/ $VERSION $DEBUG analyse_timeseries_rms /;

$VERSION = '1.00';
$DEBUG   = 1;

my @SUBCLASSES = qw/ Default GBS NGS SLS /;

use constant BAD_VALUE => 'bad';

=head1 CLASS METHODS

=over 4

=item B<analyse_timeseries_rms>

Analyse RMS values for each detector as obtained from timeseries data.

  $result = analyse_timeseries_rms( $rms );

This function tests differences in RMS between detectors. An
observation is classified as good if:

 minimum mean receptor RMS > mean RMS * ( 1 - RMSVAR_RCP )

and

 maximum mean receptor RMS < mean RMS * ( 1 + RMSVAR_RCP )

This function takes one argument, a reference to a hash with keys
being receptor names and values being the RMS for that receptor.

This function returns a reference to a hash with keys being JLS::QA
subclasses and values being the pass/fail result for this test. 0
denotes a test failure, 1 denotes a test pass.

=cut

sub analyse_timeseries_rms {
  my $rms = shift;
  my $output = shift;

  my $rmsvar_rcp_const = 'RMSVAR_RCP';
  my $rmsvar_rcp = _retrieve_constant( $rmsvar_rcp_const );

  my( $min, $max, $mean ) = _min_max_mean( $rms );

  my %result;
  foreach my $class ( sort keys %$rmsvar_rcp ) {
    if( ( $min > $mean * ( 1 - $rmsvar_rcp->{$class} ) ) &&
        ( $max < $mean * ( 1 + $rmsvar_rcp->{$class} ) ) ) {
      $result{$class} = 1;
    } else {
      $result{$class} = 0;
    }
  }

  if( defined( $output ) ) {
    $output->( "Results for RMS sensitivity variation QA:\n" );
    $output->( sprintf( " Mean value: %6.4f\n", $mean ) );
    $output->( sprintf( "  Min value: %6.4f\n", $min ) );
    $output->( sprintf( "  Max value: %6.4f\n", $max ) );
    foreach my $class ( sort keys %$rmsvar_rcp ) {
      $output->( sprintf( " %7s " . ( $result{$class} ? "passed" : "failed" ),
                          $class ) );
      $output->( sprintf( "  (%s = %.2f)\n",
                          $rmsvar_rcp_const,
                          $rmsvar_rcp->{$class} ) );
    }
  }

  return \%result;

}

sub analyse_tsysmax {
  my $tsys = shift;
  my $output = shift;

  my %result;

  my $tsysmax_const = 'TSYSMAX';

  my $tsysmax = _retrieve_constant( $tsysmax_const );

  foreach my $class ( keys %$tsysmax ) {
    $result{$class} = 1;
    foreach my $receptor ( keys %$tsys ) {
      next if $tsys->{$receptor} eq BAD_VALUE;
      if( $tsys->{$receptor} > $tsysmax->{$class} ) {
        $result{$class} = 0;
        last;
      }
    }
  }

  if( defined( $output ) ) {
    $output->( "Results for maximum Tsys value QA:\n" );
    foreach my $class ( sort keys %$tsysmax ) {
      $output->( sprintf( " %7s " . ( $result{$class} ? "passed" : "failed" ),
                          $class ) );
      $output->( sprintf( "  (%s = %.2f)\n",
                          $tsysmax_const,
                          $tsysmax->{$class} ) );
    }
  }

  return \%result;
}

sub analyse_tsysvar {
  my $tsys = shift;
  my $output = shift;

  my %result;

  my $tsysvar_const = 'TSYSVAR';
  my $tsysvar = _retrieve_constant( $tsysvar_const );

  # Determine the mean, min, and max Tsys.
  my( $min, $max, $mean ) = _min_max_mean( $tsys );

  foreach my $class ( keys %$tsysvar ) {
    if( $min > $mean * ( 1 - $tsysvar->{$class} ) &&
        $max < $mean * ( 1 + $tsysvar->{$class} ) ) {
      $result{$class} = 1;
    } else {
      $result{$class} = 0;
    }
  }

  if( defined( $output ) ) {
    $output->( "Results for Tsys variation QA:\n" );
    $output->( sprintf( " Mean value: %6.4f\n", $mean ) );
    $output->( sprintf( "  Min value: %6.4f\n", $min ) );
    $output->( sprintf( "  Max value: %6.4f\n", $max ) );
    foreach my $class ( sort keys %$tsysvar ) {
      $output->( sprintf( " %7s " . ( $result{$class} ? "passed" : "failed" ),
                          $class ) );
      $output->( sprintf( "  (%s = %.2f)\n",
                          $tsysvar_const,
                          $tsysvar->{$class} ) );
    }
  }

  return \%result;
}

sub _retrieve_constant {
  my $constant = shift;

  my $base = "JSA::QA::";
  my %return;

  foreach my $subclass ( @SUBCLASSES ) {
    my $class = $base.$subclass;
    $return{$subclass} = $class->$constant;
  }
  return \%return;
}

sub _min_max_mean {
  my $hashref = shift;

  my $sum = 0;
  my $max = 0;
  my $min = 1e6;
  my $num = 0;
  my $mean = 0;
  foreach my $value ( values %$hashref ) {
    next if $value eq BAD_VALUE;
    $sum += $value;
    if( $value > $max ) { $max = $value; }
    if( $value < $min ) { $min = $value; }
    $num++;
  }
  if( $num != 0 ) {
    $mean = $sum / $num;
  } else {
    croak "Zero good datapoints found";
  }
  return( $min, $max, $mean );
}

package JSA::QA::Default;

our @EXPORT_OK = qw/ GOODRECEP RMSVAR_RCP /;

# Number of good receptors.
use constant GOODRECEP => 13;
use constant RMSVAR_RCP => 0.5;
use constant TSYSMAX => 600;
use constant TSYSVAR => 0.3;


package JSA::QA::GBS;

use base qw/ JSA::QA::Default /;

our @EXPORT_OK = qw/ RMSVAR_RCP /;

use constant RMSVAR_RCP => 0.3;


package JSA::QA::NGS;

use base qw/ JSA::QA::Default /;

our @EXPORT_OK = qw/ /;


package JSA::QA::SLS;

use base qw/ JSA::QA::Default /;

our @EXPORT_OK = qw/ /;


1;
