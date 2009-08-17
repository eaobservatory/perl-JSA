package JSA::QA;

=head1 NAME

JSA::QA - Quality Assurance functions for ACSIS data.

=head1 SYNOPSIS

  use JSA::QA qw/ analyse_tsys /;
  my $qa = new JSA::QA( file => 'config.ini' );
  $qa->analyse_tsys( $tsys );

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
use Config::IniFiles;
use Statistics::Descriptive;

use JSA::QA::Result;

use vars qw/ $VERSION $DEBUG /;

use Exporter 'import';
our @EXPORT = qw/ $VERSION $DEBUG analyse_timeseries_rms analyse_tsys
                  analyse_tsysmax analyse_tsysvar retrieve_constant /;

$VERSION = '1.00';
$DEBUG   = 1;

use constant BAD_VALUE => 'bad';

=head1 CONSTRUCTORS

=over 4

=item B<new>

Create a JSA::QA object.

  my $qa = new JSA::QA( file => 'config.ini' );

The config file must be passed in. For config file layout, see CONFIG FILE LAYOUT section below.

=cut

sub new {
  my $proto = shift;
  my $class = ref( $proto ) || $proto;

  my %opts = @_;

  my $qa = {};

  if( exists( $opts{'file'} ) ) {
    if( -e $opts{'file'} ) {
      tie my %cfg, 'Config::IniFiles', ( -file => $opts{'file'} );
      $qa->{CONFIG} = \%cfg;
    } else {
      croak "Cannot find config file $opts{'file'}";
    }
  }

  bless( $qa, $class );
  return $qa;
}

=back

=head1 ACCESSORS

=over 4

=item B<get_data>

Retrieve quality assurance constants.

  my $goodrecept = $qa->get_data( key => 'GOODRECEP' );

Arguments are passed as key/value pairs.

The only mandatory argument is 'key'.

Optional arguments are:

 - survey: the survey for which the given QA constant applies.

 - molecule: the molecule for which the given QA constant applies. Of
 the form "13CO". Can have dashes separating atoms or isotope number
 (i.e. "13-C-O").

 - frequency: the frequency, in GHz, for which the QA constant applies.

Optional arguments are handled as follows:

 - if 'survey' alone is given, then the requested QA constant is
 returned. If no such QA constant is defined in the configuration file
 for that survey, then the default QA constant is returned.

 - if 'molecule' is given, that takes priority over 'frequency'. If
 the given survey has the molecule defined, then that QA constant is
 returned. If not, then the default QA constant for that survey is
 returned. If that is not defined, then the default QA constant for
 that molecule is returned. If that is not defined, then the default
 QA constant is returned.

 - if 'frequency' is given, and the given survey has a defined range
 containing that frequency, then that QA constant is returned. If not,
 but the survey is defined, then the default QA constant for that
 survey is returned. If that is not defined, then the default QA
 constant for the frequency range containing the given frequency is
 returned. If that is not defined, then the default QA constant is
 returned.

No translation between molecule and frequency is done.

=back

=cut

sub get_data {
  my $self = shift;
  my %opts = @_;

  foreach my $optkey ( keys %opts ) {
    $opts{lc($optkey)} = $opts{$optkey};
  }

  return unless defined $opts{'key'};

  my $key = uc( $opts{'key'} );

  if( ! defined( $opts{'survey'} ) ) {
    # Return the default.
    return $self->{'CONFIG'}->{'default'}->{$key};
  }

  my $survey = uc( $opts{'survey'} );

  if( ! defined( $opts{'molecule'} ) &&
      ! defined( $opts{'frequency'} ) ) {

    # Return the survey, if it exists. Otherwise, return the default.
    if( exists( $self->{'CONFIG'}->{$survey} ) &&
        exists( $self->{'CONFIG'}->{$survey}->{$key} ) ) {
      return $self->{'CONFIG'}->{$survey}->{$key};
    } else {
      return $self->{'CONFIG'}->{'default'}->{$key};
    }
  }

  if( defined( $opts{'molecule'} ) ) {
    my $molecule = uc( $opts{'molecule'} );

    # Remove any dashes.
    $molecule =~ s/-//g;

    # Bodge together the survey and the molecule name.
    my $str = "$survey $molecule";

    # First search for this combination. If it doesn't exist, fall
    # back to survey. If that doesn't exist, search for the default
    # value for this molecule. If that doesn't exist, we try the
    # frequency.
    if( exists( $self->{'CONFIG'}->{$str} ) &&
        exists( $self->{'CONFIG'}->{$str}->{$key} ) ) {
      return $self->{'CONFIG'}->{$str}->{$key};
    } elsif( exists( $self->{'CONFIG'}->{$survey} ) &&
             exists( $self->{'CONFIG'}->{$survey}->{$key} ) ) {
      return $self->{'CONFIG'}->{$survey}->{$key};
    } elsif( exists( $self->{'CONFIG'}->{"default $molecule"} ) &&
             exists( $self->{'CONFIG'}->{"default $molecule"}->{$key} ) ) {
      return $self->{'CONFIG'}->{"default $molecule"}->{$key};
    }
  }

  # Now for frequencies.  Get config keys that have the requested
  # survey and a colon.
  if( defined( $opts{'frequency'} ) ) {
    my $frequency = $opts{'frequency'};
    my @config_keys = grep { /:/ } grep { /$survey/ } keys %{$self->{'CONFIG'}};
    foreach my $config_key ( @config_keys ) {
      $config_key =~ /(\d+):(\d+)/;
      my $lower = $1;
      my $upper = $2;
      if( $lower < $frequency && $frequency < $upper ) {
        return $self->{'CONFIG'}->{$config_key}->{$key};
      }
    }
  }

  # If we made it here, there isn't an entry matching this frequency
  # for this survey. Fall back to the survey default.
  if( exists( $self->{'CONFIG'}->{$survey} ) &&
      exists( $self->{'CONFIG'}->{$survey}->{$key} ) ) {
    return $self->{'CONFIG'}->{$survey}->{$key};
  }

  # If we made it here, there isn't an entry for this survey. Try the
  # default, looking for frequency ranges.
  if( defined( $opts{'frequency'} ) ) {
    my $frequency = $opts{'frequency'};
    my @config_keys = grep { /:/ } grep { /default/ } keys %{$self->{'CONFIG'}};
    foreach my $config_key ( @config_keys ) {
      $config_key =~ /(\d+):(\d+)/;
      my $lower = $1;
      my $upper = $2;
      if( $lower < $frequency && $frequency < $upper &&
          defined( $self->{'CONFIG'}->{$config_key}->{$key} ) ) {
        return $self->{'CONFIG'}->{$config_key}->{$key};
      }
    }
  }

  # If we made it here, just return the default.
  return $self->{'CONFIG'}->{'default'}->{$key};

}

=head1 FUNCTIONS

=over 4

=item B<analyse_timeseries_rms>

Analyse RMS values for each receptor as obtained from timeseries data.

  $result = $qa->analyse_timeseries_rms( $rms );

This function tests differences in RMS between receptors. An
observation is classified as good if:

 minimum mean receptor RMS > mean RMS * ( 1 - RMSVAR_RCP )

and

 maximum mean receptor RMS < mean RMS * ( 1 + RMSVAR_RCP )

This function takes one argument, a reference to a hash with keys
being receptor names and values being the RMS for that receptor.

An optional hash can be passed to denote that only a specific survey
should be tested:

 $result = $qa->analyse_timeseries_rms( $rms, 'survey' => 'GBS' );

Other allowed parameters are 'molecule', 'frequency', and
'iterate'. 'molecule' and 'frequency' define a specific molecule or
frequency (in GHz) to use. If 'iterate' is true, then the analysis
will trim out failing receptors until either the number of remaining
good receptors is below the GOODRECEP value for the given
survey/frequency/molecule, or the test passes.

This function returns a JLS::QA::Result object.

=cut

sub analyse_timeseries_rms {
  my $self = shift;
  my $rms = shift;
  my %opts = @_;

  my $iterate = ( defined( $opts{'iterate'} ) ? $opts{'iterate'} : 0 );

  my $rmsvar_rcp_const = 'RMSVAR_RCP';
  my $rmsvar_rcp = $self->get_data( key => $rmsvar_rcp_const, %opts );

  my $mmm_return = _min_max_mean( [ values %$rms ] );

  my $result = new JSA::QA::Result;

  if( defined( $mmm_return->{min} ) ) {
    $result->rms_stats( $mmm_return );
  }
  if( ! defined( $mmm_return->{min} ) ||
      ( $mmm_return->{min} < $mmm_return->{mean} * ( 1 - $rmsvar_rcp ) ) ||
      ( $mmm_return->{max} > $mmm_return->{mean} * ( 1 + $rmsvar_rcp ) ) ) {

      $result->pass( 0 );
      my $fail_reason = sprintf( "Receptor-to-receptor RMS values varied by more than %d%%\n", int( $rmsvar_rcp * 100 ) );
      $fail_reason .= sprintf( "  [min=%.2f (-%.2f%% of mean) max=%.2f (+%.2f%% of mean) mean=%.2f]",
                               $mmm_return->{min},
                               abs( $mmm_return->{min} - $mmm_return->{mean} ) / $mmm_return->{mean} * 100,
                               $mmm_return->{max},
                               abs( $mmm_return->{max} - $mmm_return->{mean} ) / $mmm_return->{mean} * 100,
                               $mmm_return->{mean}
                             );
      $result->add_fail_reason( $fail_reason );
  }

  if( ! $result->pass && $iterate ) {

    # First find out how many good receptors we can have.
    my $goodrecep_const = 'GOODRECEP';
    my $goodrecep = $self->get_data( key => $goodrecep_const, %opts );

    # Count the number of good receptors.
    my $numgood = grep { !/bad/ } values %$rms;

    my $newrms = $rms;
    my $mmm2;
    my @receptors_removed;

    if( $numgood <= $goodrecep ) {
      my $note = "Cannot iterate by removing high-RMS receptors as there are $numgood good receptors and requested number of good receptors is $goodrecep.";
      $result->add_note( $note );
    } else {

      while( $numgood >= $goodrecep ) {

        # Find the highest RMS, knock it out.
        my $highest_recep = _highest( $newrms );
        push @receptors_removed, $highest_recep;
        delete $newrms->{$highest_recep};
        $result->add_bad_receptor( $highest_recep );
        # Get the stats on the remaining receptors.
        $mmm2 = _min_max_mean( [ values %$newrms ] );

        # Check to see if we pass now.
        if( defined( $mmm2->{min} ) &&
            ( $mmm2->{min} > $mmm2->{mean} * ( 1 - $rmsvar_rcp ) ) &&
            ( $mmm2->{max} < $mmm2->{mean} * ( 1 + $rmsvar_rcp ) ) ) {
          $result->pass( 1 );
          $result->rms_stats( $mmm2 );
          last;
        } else {
          $numgood--;
        }
      }

      if( $result->pass ) {
        $result->clear_fail_reasons;
        my $note = "Receptor-to-receptor RMS value test passed after removing receptor" . ( scalar( @receptors_removed ) > 1 ? 's ' : ' ' ) . join ',', sort @receptors_removed;
        $result->add_note( $note );
      } else {
        my $fail_reason = sprintf( "Receptor-to-receptor RMS values varied by more than %d%% after removing high-RMS receptors.\n", int( $rmsvar_rcp * 100 ) );
        $fail_reason .= sprintf( "  [min=%.2f (-%.2f%% of mean) max=%.2f (+%.2f%% of mean) mean=%.2f]\n",
                                 $mmm2->{min},
                                 abs( $mmm2->{min} - $mmm2->{mean} ) / $mmm2->{mean} * 100,
                                 $mmm2->{max},
                                 abs( $mmm2->{max} - $mmm2->{mean} ) / $mmm2->{mean} * 100,
                                 $mmm2->{mean}
                               );
        $fail_reason .= "  Receptors removed: " . join ',', @receptors_removed;
        $result->add_fail_reason( $fail_reason );
      }
    }
  }

  return $result;

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
  my $self = shift;
  my $tsys = shift;
  my %opts = @_;

  # First, analyse the Tsys based on a maximum threshold.
  my $result = $self->analyse_tsysmax( $tsys, %opts );

  # We now have a list of bad receptors that didn't pass the Tsys
  # threshold test, so create a new Tsys hash with the survey-specific
  # thresholded receptors set to BAD_VALUE, and come up with the
  # receptor to receptor variance pass/fail, using only the good
  # receptors, but only if we actually passed.
  if( $result->pass ) {

    my %bad_receptors = map { $_, 1 } @{$result->bad_receptors};

    # Create the temporary Tsys values, excluding the bad receptors.
    my %temp_tsys;
    foreach my $receptor ( sort keys %$tsys ) {
      next if $tsys->{$receptor} eq BAD_VALUE;
      next if exists $bad_receptors{$receptor};
      $temp_tsys{$receptor} = $tsys->{$receptor};
    }

    my $tresult;
    if( scalar keys %temp_tsys == 0 ) {
      $tresult = new JSA::QA::Result( 'pass' => 0 );

      my $tsysbad_const = 'TSYSBAD';

      my $tsysbad = $self->get_data( key => $tsysbad_const, %opts );

      my $fail_reason = "All receptors have Tsys greater than $tsysbad";
      $tresult->add_fail_reason( $fail_reason );

    } else {
      $tresult = $self->analyse_tsysvar( \%temp_tsys, %opts );
    }

    my $merged = $result->merge( $tresult );

    return $merged;
  } else {
    return $result;
  }
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

This function returns a JLS::QA::Result object.

=cut

sub analyse_tsysmax {
  my $self = shift;
  my $tsys = shift;
  my %opts = @_;

  my $result = new JSA::QA::Result( 'pass' => 1 );

  my $tsysbad_const = 'TSYSBAD';

  my $tsysbad = $self->get_data( key => $tsysbad_const, %opts );

  my @bad_receptors;
  foreach my $receptor ( keys %$tsys ) {
    if( defined( $tsys->{$receptor} ) &&
        $tsys->{$receptor} eq BAD_VALUE or
        $tsys->{$receptor} > $tsysbad ) {
      $result->add_bad_receptor( $receptor );
    }
  }

  # Check the number of good receptors against the allowed number of
  # good receptors.
  my $numgood = ( scalar keys %$tsys ) - ( scalar @{$result->bad_receptors()} );
  my $goodrecep_const = 'GOODRECEP';
  my $goodrecep = $self->get_data( key => $goodrecep_const, %opts );
  if( $numgood < $goodrecep ) {
    $result->pass( 0 );
    my $reason = "Requested number of good receptors below ${tsysbad}K is $goodrecep, but only $numgood receptors were below this threshold.";
    $result->add_fail_reason( $reason );
  }

  return $result;
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
  my $self = shift;
  my $tsys = shift;
  my %opts = @_;

  my $iterate = ( defined( $opts{'iterate'} ) ? $opts{'iterate'} : 0 );

  my $result;

  my $tsysvar_const = 'TSYSVAR';
  my $tsysvar = $self->get_data( key => $tsysvar_const, %opts );
  my $tsysmax_const = 'TSYSMAX';
  my $tsysmax = $self->get_data( key => $tsysmax_const, %opts );

  # Determine the mean, min, and max Tsys.
  my $mmm_return = _min_max_mean( [ values %$tsys ] );

  $result = new JSA::QA::Result( pass => 1 );
  if( defined( $mmm_return->{min} ) ) {
    $result->tsys_stats( $mmm_return );
  }
  if( ! defined( $mmm_return->{min} ) ||
      $mmm_return->{min} < $mmm_return->{mean} * ( 1 - $tsysvar ) ||
      $mmm_return->{max} > $mmm_return->{mean} * ( 1 + $tsysvar ) ) {
    $result->pass( 0 );
    my $fail_reason = sprintf( "Receptor-to-receptor Tsys values varied by more than %d%%\n", int( $tsysvar * 100 ) );
    $fail_reason .= sprintf( "  [min=%.2f (-%.2f%% of mean) max=%.2f (+%.2f%% of mean) mean=%.2f]",
                             $mmm_return->{min},
                             abs( $mmm_return->{min} - $mmm_return->{mean} ) / $mmm_return->{mean} * 100,
                             $mmm_return->{max},
                             abs( $mmm_return->{max} - $mmm_return->{mean} ) / $mmm_return->{mean} * 100,
                             $mmm_return->{mean}
                           );

    $result->add_fail_reason( $fail_reason );
  }

  if( defined( $mmm_return->{mean} ) &&
      $mmm_return->{mean} > $tsysmax ) {
    $result->pass( 0 );
    my $fail_reason = sprintf( "Mean Tsys value (%.2f) is greater than maximum allowed value (%d)",
                               $mmm_return->{mean},
                               $tsysmax
                               );
    $result->add_fail_reason( $fail_reason );
  }

  # Iterate, if requested, and if we failed the first time through.
  if( ! $result->pass && $iterate ) {

    # First, find out how many good receptors we can have.
    my $goodrecep_const = 'GOODRECEP';
    my $goodrecep = $self->get_data( key => $goodrecep_const, %opts );

    # Count the number of good receptors.
    my $numgood = grep { !/bad/ } values %$tsys;

    my $newtsys = $tsys;
    my $mmm2;
    my @receptors_removed;

    if( $numgood <= $goodrecep ) {
      my $note = "Cannot iterate by removing high-Tsys receptors as there are $numgood good receptors and requested number of good receptors is $goodrecep.";
      $result->add_note( $note );
    } else {

      while( $numgood >= $goodrecep ) {

        # Find the highest Tsys, knock it out.
        my $highest_recep = _highest( $newtsys );
        push @receptors_removed, $highest_recep;
        delete $newtsys->{$highest_recep};
        $result->add_bad_receptor( $highest_recep );

        #Get stats on the remaining receptors.
        $mmm2 = _min_max_mean( [ values %$newtsys ] );

        # Check to see if we pass now.
        if( defined( $mmm2->{min} ) &&
            defined( $mmm2->{mean} ) &&
            ( $mmm2->{min} > $mmm2->{mean} * ( 1 - $tsysvar ) ) &&
            ( $mmm2->{max} < $mmm2->{mean} * ( 1 + $tsysvar ) ) &&
            ( $mmm2->{mean} < $tsysmax ) ) {
          $result->pass( 1 );
          $result->tsys_stats( $mmm2 );
          last;
        } else {
          $numgood--;
        }
      }

      if( $result->pass ) {
        $result->clear_fail_reasons;
        my $note = "Receptor-to-receptor Tsys value test passed after removing receptor" . ( scalar( @receptors_removed ) > 1 ? 's ' : ' ' ) . ( join ',', sort @receptors_removed ) . ".";
        $result->add_note( $note );
      } else {
        my $fail_reason = "Receptor-to-receptor Tsys value test still failed after removing receptor" . ( scalar( @receptors_removed ) > 1 ? 's ' : ' ' ) . ( join ',', sort @receptors_removed ) . ".";
        $result->add_fail_reason( $fail_reason );
      }
    }
  }

  return $result;
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

  my @values = grep { $_ ne BAD_VALUE } @$values;

  my $stat = Statistics::Descriptive::Sparse->new();
  $stat->add_data( @values );

  if( $stat->count != 0 ) {
    $return{'min'} = $stat->min();
    $return{'max'} = $stat->max();
    $return{'mean'} = $stat->mean();
  }

  return \%return;
}

=item B<_highest>

=cut

sub _highest {
  my $href = shift;

  my $highest_recep = '';
  my $highest = 0;

  foreach my $recep ( keys %$href ) {
    if( $href->{$recep} ne BAD_VALUE && $href->{$recep} > $highest ) {
      $highest = $href->{$recep};
      $highest_recep = $recep;
    }
  }

  return $highest_recep;

}



package JSA::QA::Telescope;

our @EXPORT_OK = qw/ GOODRECEP RMSTSYSTOL RMSTSYSTOL_QUEST
                     RMSTSYSTOL_FAIL RMSVAR_RCP TSYSBAD TSYSMAX
                     TSYSVAR /;

# Number of good receptors.
use constant GOODRECEP => 13;

# Fractional variation between RMS and Tsys.
use constant RMSTSYSTOL => 0.3;

# Lower limit of fraction of spectra failing RMS/Tsys consistency
# check for questionable status.
use constant RMSTSYSTOL_QUEST => 0.025;

# Lower limit of fraction of spectra failing RMS/Tsys consistency
# check for failed status.
use constant RMSTSYSTOL_FAIL => 0.05;

# Fractional variation in RMS across the map.
use constant RMSVAR_MAP => 0.6;

# Fractional variation in RMS numbers.
use constant RMSVAR_RCP => 1.0;

# Fractional variation in RMS with frequency.
use constant RMSVAR_SPEC => 0.2;

# Tsys limit above which a receptor is flagged as bad.
use constant TSYSBAD => 1500;

# Mean Tsys for good receptor limit.
use constant TSYSMAX => 750;

# Fractional variation in Tsys numbers.
use constant TSYSVAR => 1.0;


package JSA::QA::GBS;

use base qw/ JSA::QA::Telescope /;

our @EXPORT_OK = qw/ RMSTSYSTOL RMSTSYSTOL_QUEST RMSTSYSTOL_FAIL
                     RMSVAR_RCP TSYSBAD TSYSMAX TSYSVAR /;

use constant RMSTSYSTOL_QUEST => 0.03;
use constant RMSTSYSTOL_FAIL => 0.05;
use constant RMSVAR_RCP => 0.5;
use constant TSYSBAD => 1200;
use constant TSYSMAX => 600;
use constant TSYSVAR => 0.5;


package JSA::QA::NGS;

# These are temporarily in place for GBS C18O

use base qw/ JSA::QA::Telescope /;

our @EXPORT_OK = qw/ RMSVAR_RCP TSYSVAR /;

use constant RMSVAR_RCP => 1.0;
use constant TSYSVAR => 1.0;


package JSA::QA::SLS;

use base qw/ JSA::QA::Telescope /;

our @EXPORT_OK = qw/ /;


1;
