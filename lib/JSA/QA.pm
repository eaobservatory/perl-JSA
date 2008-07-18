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
                  analyse_tsysmax analyse_tsysvar retrieve_constant /;

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
  my $rmsvar_rcp = retrieve_constant( $rmsvar_rcp_const, $survey_opt );

  my $mmm_return = _min_max_mean( [ values %$rms ] );

  my %result;
  foreach my $survey ( sort keys %$rmsvar_rcp ) {
    $result{$survey} = new JSA::QA::Result;
    if( defined( $mmm_return->{min} ) ) {
      $result{$survey}->rms_stats( $mmm_return );
    }
    if( ! defined( $mmm_return->{min} ) ||
        ( $mmm_return->{min} < $mmm_return->{mean} * ( 1 - $rmsvar_rcp->{$survey} ) ) ||
        ( $mmm_return->{max} > $mmm_return->{mean} * ( 1 + $rmsvar_rcp->{$survey} ) ) ) {

      $result{$survey}->pass( 0 );
      my $fail_reason = sprintf( "Receptor-to-receptor RMS values varied by more than %d%%\n", int( $rmsvar_rcp->{$survey} * 100 ) );
      $fail_reason .= sprintf( "  [min=%.2f (-%.2f%% of mean) max=%.2f (+%.2f%% of mean) mean=%.2f]",
                               $mmm_return->{min},
                               abs( $mmm_return->{min} - $mmm_return->{mean} ) / $mmm_return->{mean} * 100,
                               $mmm_return->{max},
                               abs( $mmm_return->{max} - $mmm_return->{mean} ) / $mmm_return->{mean} * 100,
                               $mmm_return->{mean}
                             );
      $result{$survey}->add_fail_reason( $fail_reason );
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
                                   'survey' => $survey );

    $result{$survey}->tsys_stats( $tresult->{$survey}->tsys_stats );

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
a given survey, then it is flagged as bad and removed from
processing. Any receptors exceeding this maximum value are returned in
the result object for that survey.

This function takes one argument, a reference to a hash with keys
being receptor names and values being the RMS for that receptor.

An optional hash can be passed to denote that only a specific survey
should be tested:

  $result = analyse_tsysmax( $rms, 'survey' => 'GBS' );

This function returns a reference to a hash with keys being JLS::QA
survey subclasses and values being JLS::QA::Result objects.

The returned JLS::QA::Result objects for this function will have their
pass() accessor set to true, but will have their bad_receptors()
accessor filled with any receptors exceeding the maximum value.

=cut

sub analyse_tsysmax {
  my $tsys = shift;
  my %options = @_;

  my $survey_opt = $options{'survey'};

  my %result;

  my $tsysbad_const = 'TSYSBAD';

  my $tsysbad = retrieve_constant( $tsysbad_const, $survey_opt );

  foreach my $survey ( keys %$tsysbad ) {
    $result{$survey} = new JSA::QA::Result( 'pass' => 1 );
    my @bad_receptors;
    foreach my $receptor ( keys %$tsys ) {
      next if $tsys->{$receptor} eq BAD_VALUE;
      if( $tsys->{$receptor} > $tsysbad->{$survey} ) {
        $result{$survey}->add_bad_receptor( $receptor );
      }
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
  my $tsysvar = retrieve_constant( $tsysvar_const, $survey_opt );
  my $tsysmax_const = 'TSYSMAX';
  my $tsysmax = retrieve_constant( $tsysmax_const, $survey_opt );

  # Determine the mean, min, and max Tsys.
  my $mmm_return = _min_max_mean( [ values %$tsys ] );

  foreach my $survey ( keys %$tsysvar ) {
    $result{$survey} = new JSA::QA::Result( pass => 1 );
    if( defined( $mmm_return->{min} ) ) {
      $result{$survey}->tsys_stats( $mmm_return );
    }
    if( ! defined( $mmm_return->{min} ) ||
        $mmm_return->{min} < $mmm_return->{mean} * ( 1 - $tsysvar->{$survey} ) ||
        $mmm_return->{max} > $mmm_return->{mean} * ( 1 + $tsysvar->{$survey} ) ) {
      $result{$survey}->pass( 0 );
      my $fail_reason = sprintf( "Receptor-to-receptor Tsys values varied by more than %d%%\n", int( $tsysvar->{$survey} * 100 ) );
      $fail_reason .= sprintf( "  [min=%.2f (-%.2f%% of mean) max=%.2f (+%.2f%% of mean) mean=%.2f]",
                               $mmm_return->{min},
                               abs( $mmm_return->{min} - $mmm_return->{mean} ) / $mmm_return->{mean} * 100,
                               $mmm_return->{max},
                               abs( $mmm_return->{max} - $mmm_return->{mean} ) / $mmm_return->{mean} * 100,
                               $mmm_return->{mean}
                             );

      $result{$survey}->add_fail_reason( $fail_reason );
    }
    if( defined( $mmm_return->{max} ) &&
        $mmm_return->{max} > $tsysmax->{$survey} ) {
      $result{$survey}->pass( 0 );
      my $fail_reason = sprintf( "Mean Tsys value (%.2f) is greater than maximum allowed value (%d)\n",
                                 $mmm_return->{max},
                                 $tsysmax->{$survey}
                               );
      $result{$survey}->add_fail_reason( $fail_reason );
    }
  }

  return \%result;
}

=item B<retrieve_constant>

Retrieve the requested named constant from a JLS::QA survey subclass.

  $value = retrieve_constant( 'RMSVAR_RCP' );
  $value = retrieve_constant( 'RMSVAR_RCP', 'GBS' );

This function takes two arguments, the constant to be returned and an
optional survey for which the constant is to be returned.

This function returnsa reference to a hash with keys being JLS::QA
survey subclasses and values being the requested constant value for
that survey.

=cut

sub retrieve_constant {
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

=back

=head1 PRIVATE FUNCTIONS

=over 4

=item B<_min_max_mean>

Return minimum, maximum, and mean values for a set of numbers.

  $result = _min_max_mean( $values );

This function takes a reference to an array of numbers to be
tested. If any of the values are equal to the BAD_VALUE constant, they
are skipped and not used for statistics (i.e. if one value is
BAD_VALUE then the mean is ( sum / (N - 1) )).

This value returns a reference to a hash with keys being 'min', 'max',
and 'mean', and values corresponding to the values for each key. If no
good values are present, then all values returned in the hash will be
'undef'.

=cut

sub _min_max_mean {
  my $values = shift;

  my %return = ( 'max' => undef,
                 'min' => undef,
                 'mean' => undef );
  my $min = undef;
  my $max = undef;
  my $mean = undef;

  my $num = 0;
  my $sum = 0;

  foreach my $value ( @$values ) {
    next if $value eq BAD_VALUE;
    $sum += $value;
    if( ! defined( $max ) || $value > $max ) { $max = $value; }
    if( ! defined( $min ) || $value < $min ) { $min = $value; }
    $num++;
  }
  if( $num != 0 ) {
    $mean = $sum / $num;
  }

  $return{'min'} = $min;
  $return{'max'} = $max;
  $return{'mean'} = $mean;

  return \%return;
}





package JSA::QA::Telescope;

our @EXPORT_OK = qw/ GOODRECEP RMSVAR_RCP TSYSMAX TSYSVAR /;

# Number of good receptors.
use constant GOODRECEP => 13;

# Fractional variation in RMS numbers.
use constant RMSVAR_RCP => 1.0;

# Tsys limit above which a receptor is flagged as bad.
use constant TSYSBAD => 1500;

# Mean Tsys for good receptor limit.
use constant TSYSMAX => 750;

# Fractional variation in Tsys numbers.
use constant TSYSVAR => 1.0;


package JSA::QA::GBS;

use base qw/ JSA::QA::Telescope /;

our @EXPORT_OK = qw/ RMSVAR_RCP TSYSMAX /;

use constant RMSVAR_RCP => 0.3;
use constant TSYSBAD => 1200;
use constant TSYSMAX => 600;
use constant TSYSVAR => 0.3;


package JSA::QA::NGS;

use base qw/ JSA::QA::Telescope /;

our @EXPORT_OK = qw/ /;

use constant RMSVAR_RCP => 0.5;
use constant TSYSVAR => 0.5;


package JSA::QA::SLS;

use base qw/ JSA::QA::Telescope /;

our @EXPORT_OK = qw/ /;


1;
