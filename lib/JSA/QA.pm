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

use JSA::QA::Result;

use vars qw/ $VERSION $DEBUG /;

use Exporter 'import';
our @EXPORT = qw/ $VERSION $DEBUG analyse_timeseries_rms analyse_tsys
                  analyse_tsysmax analyse_tsysvar /;

$VERSION = '1.00';
$DEBUG   = 1;

my @SUBCLASSES = qw/ Telescope GBS NGS SLS /;

use constant BAD_VALUE => 'bad';

=head1 FUNCTIONS

=over 4

=item B<analyse_timeseries_rms>

Analyse RMS values for each receptor as obtained from timeseries data.

  $result = analyse_timeseries_rms( $rms );

This function tests differences in RMS between receptors. An
observation is classified as good if:

 minimum mean receptor RMS > mean RMS * ( 1 - RMSVAR_RCP )

and

 maximum mean receptor RMS < mean RMS * ( 1 + RMSVAR_RCP )

This function takes one argument, a reference to a hash with keys
being receptor names and values being the RMS for that receptor.

An optional hash can be passed to denote that only a specific survey
should be tested:

 $result = analyse_timeseries_rms( $rms, 'survey' => 'GBS' );

This function returns a reference to a hash with keys being JLS::QA
survey subclasses and values being JLS::QA::Result objects.

=cut

sub analyse_timeseries_rms {
  my $rms = shift;
  my %options = @_;

  my $survey_opt = $options{'survey'};

  my $rmsvar_rcp_const = 'RMSVAR_RCP';
  my $rmsvar_rcp = _retrieve_constant( $rmsvar_rcp_const, $survey_opt );

  my( $min, $max, $mean ) = _min_max_mean( values %$rms );

  my %result;
  foreach my $survey ( sort keys %$rmsvar_rcp ) {
    $result{$survey} = new JSA::QA::Result;
    if( ( $min < $mean * ( 1 - $rmsvar_rcp->{$survey} ) ) ||
        ( $max > $mean * ( 1 + $rmsvar_rcp->{$survey} ) ) ) {

      $result{$survey}->pass( 0 );
      $result{$survey}->add_fail_reason( "Receptor-to-receptor RMS values varied by more than " . int( $rmsvar_rcp->{$survey} * 100 ) . "%" );
    }
  }

  return \%result;

}

=item B<analyse_tsys>

Analyse Tsys values for a given observation.

  $result = analyse_tsys( $rms );

This function performs two tests in succession. First, it runs the
analyse_tsysmax() function. In doing so it temporarily removes any
receptors failing that test, and then runs the analyse_tsysvar()
function.

This function takes one argument, a reference to a hash with keys
being receptor names and values being the mean Tsys for that receptor.

An optional hash can be passed to denote that only a specific survey
should be tested:

  $result = analyse_tsys( $rms, 'survey' => 'GBS' );

This function returns a reference to a hash with keys being JLS::QA
survey subclasses and values bing JLS::QA::Result objects.

=cut

sub analyse_tsys {
  my $tsys = shift;
  my %options = @_;

  my %result; # Result hash to be returned to caller.

  # First, analyse the Tsys based on a maximum threshold.
  my $result = analyse_tsysmax( $tsys, %options );

  # We now have a list of bad receptors that didn't pass the Tsys
  # threshold test, so go through each survey in the results, create a
  # new Tsys hash with the survey-specific thresholded receptors set
  # to BAD_VALUE, and come up with the receptor to receptor variance
  # pass/fail, using only the good receptors.
  foreach my $survey ( keys %$result ) {

    my %bad_receptors = map { $_, 1 } @{$result->{$survey}->bad_receptors};
    $result{$survey} = $result->{$survey};

    # Create the temporary Tsys values, excluding the bad receptors.
    my %temp_tsys;
    foreach my $receptor ( sort keys %$tsys ) {
      next if $tsys->{$receptor} eq BAD_VALUE;
      next if exists $bad_receptors{$receptor};
      $temp_tsys{$receptor} = $tsys->{$receptor};
    }

    my $tresult = analyse_tsysvar( \%temp_tsys,
                                   'survey' => $options{'survey'} );

    if( ! $tresult->{$survey}->pass ) {
      $result{$survey}->pass( 0 );
      $result{$survey}->add_fail_reason( @{$tresult->{$survey}->fail_reasons} );
    }

  }

  return \%result;

}

=item B<analyse_tsysmax>

Analyse Tsys values for each receptor, checking if they exceed a given
threshold.

  $result = analyse_tsysmax( $rms );

This function compares the Tsys value for each receptor with a maximum
value. If the Tsys for a given receptor exceeds that maximum value for
a given survey, then this test fails for that survey. Any receptors
exceeding the maximum value are returned in the result object for that
survey.

This function takes one argument, a reference to a hash with keys
being receptor names and values being the RMS for that receptor.

An optional hash can be passed to denote that only a specific survey
should be tested:

  $result = analyse_tsysmax( $rms, 'survey' => 'GBS' );

This function returns a reference to a hash with keys being JLS::QA
survey subclasses and values being JLS::QA::Result objects.

=cut

sub analyse_tsysmax {
  my $tsys = shift;
  my %options = @_;

  my $survey_opt = $options{'survey'};

  my %result;

  my $tsysmax_const = 'TSYSMAX';

  my $tsysmax = _retrieve_constant( $tsysmax_const, $survey_opt );

  foreach my $survey ( keys %$tsysmax ) {
    $result{$survey} = new JSA::QA::Result;
    my @bad_receptors;
    foreach my $receptor ( keys %$tsys ) {
      next if $tsys->{$receptor} eq BAD_VALUE;
      if( $tsys->{$receptor} > $tsysmax->{$survey} ) {
        $result{$survey}->pass( 0 );
        $result{$survey}->add_bad_receptor( $receptor );
      }
    }
    if( ! $result{$survey}->pass ) {
      my $fail_reason = "Receptor" . ( $#{$result{$survey}->bad_receptors} > 0 ? "s " : " " );
      $fail_reason .= ( join ",", sort @{$result{$survey}->bad_receptors} );
      $fail_reason .= " had Tsys higher than " . $tsysmax->{$survey} . "K";
      $result{$survey}->add_fail_reason( $fail_reason );
    }
  }

  return \%result;
}

=item B<analyse_tsysvar>

Analyse Tsys values for each receptor, checking minimum and maximum
values against the mean value across the array.

  $result = analyse_tsysvar( $rms );

This function tests differences in Tsys between receptors. An
observation is classified as good if:

 minimum mean receptor Tsys > mean Tsys * ( 1 - TSYSVAR )

and

 maximum mean receptor Tsys < mean Tsys * ( 1 + TSYSVAR )

This function takes one argument, a reference to a hash with keys
being receptor names and values being the Tsys for that receptor.

An optional hash can be passed to denote that only a specific survey
should be tested:

 $result = analyse_timeseries_rms( $rms, 'survey' => 'GBS' );

This function returns a reference to a hash with keys being JLS::QA
survey subclasses and values being JLS::QA::Result objects.

=cut

sub analyse_tsysvar {
  my $tsys = shift;
  my %options = @_;

  my $survey_opt = $options{'survey'};

  my %result;

  my $tsysvar_const = 'TSYSVAR';
  my $tsysvar = _retrieve_constant( $tsysvar_const, $survey_opt );

  # Determine the mean, min, and max Tsys.
  my( $min, $max, $mean ) = _min_max_mean( values %$tsys );

  foreach my $survey ( keys %$tsysvar ) {
    $result{$survey} = new JSA::QA::Result( pass => 1 );
    if( $min < $mean * ( 1 - $tsysvar->{$survey} ) ||
        $max > $mean * ( 1 + $tsysvar->{$survey} ) ) {
      $result{$survey}->pass( 0 );
      $result{$survey}->add_fail_reason( "Receptor-to-receptor Tsys varied by more than " . int( $tsysvar->{$survey} * 100 ) . "%" );
    }
  }

  return \%result;
}

=back

=head1 PRIVATE FUNCTIONS

=over 4

=item B<_retrieve_constant>

Retrieve the requested named constant from a JLS::QA survey subclass.

  $value = _retrieve_constant( 'RMSVAR_RCP' );
  $value = _retrieve_constant( 'RMSVAR_RCP', 'GBS' );

This function takes two arguments, the constant to be returned and an
optional survey for which the constant is to be returned.

This function returnsa reference to a hash with keys being JLS::QA
survey subclasses and values being the requested constant value for
that survey.

=cut

sub _retrieve_constant {
  my $constant = shift;
  my $requested_class;

  if( @_ ) { $requested_class = shift; }

  my $base = "JSA::QA::";
  my %return;

  if( defined( $requested_class ) ) {
    my $class = $base.$requested_class;
    $return{$requested_class} = $class->$constant;
  } else {
    foreach my $subclass ( @SUBCLASSES ) {
      my $class = $base.$subclass;
      $return{$subclass} = $class->$constant;
    }
  }
  return \%return;
}

=item B<_min_max_mean>

Return minimum, maximum, and mean values for a set of numbers.

  ( $min, $max, $mean ) = _min_max_mean( @values );

This function takes a list of numbers to be tested. If any of the
values are equal to the BAD_VALUE constant, they are skipped and not
used for statistics (i.e. if one value is BAD_VALUE then the mean is (
sum / (N - 1) )).

This value returns a list of the minimum, maximum, and mean values, in
that order.

=cut

sub _min_max_mean {
  my @values = @_;

  my $max = undef;
  my $min = undef;
  my $num = 0;
  my $mean = 0;
  my $sum = 0;

  foreach my $value ( @values ) {
    next if $value eq BAD_VALUE;
    $sum += $value;
    if( ! defined( $max ) || $value > $max ) { $max = $value; }
    if( ! defined( $min ) || $value < $min ) { $min = $value; }
    $num++;
  }
  if( $num != 0 ) {
    $mean = $sum / $num;
  } else {
    croak "Zero good datapoints found";
  }

  return( $min, $max, $mean );
}





package JSA::QA::Telescope;

our @EXPORT_OK = qw/ GOODRECEP RMSVAR_RCP TSYSMAX TSYSVAR /;

# Number of good receptors.
use constant GOODRECEP => 13;
use constant RMSVAR_RCP => 3;
use constant TSYSMAX => 15000;
use constant TSYSVAR => 1.0;


package JSA::QA::GBS;

use base qw/ JSA::QA::Telescope /;

our @EXPORT_OK = qw/ RMSVAR_RCP TSYSMAX /;

use constant RMSVAR_RCP => 0.3;
use constant TSYSMAX => 600;
use constant TSYSVAR => 0.3;

package JSA::QA::NGS;

use base qw/ JSA::QA::Telescope /;

our @EXPORT_OK = qw/ /;


package JSA::QA::SLS;

use base qw/ JSA::QA::Telescope /;

our @EXPORT_OK = qw/ /;


1;
