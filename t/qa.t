#!perl

use Test::More tests => 19;
use Data::Dumper;

my @surveys = qw/ NGS Telescope GBS SLS /;

use_ok( "JSA::QA" );

my %rms = ( 'H00' => 1.977,
            'H01' => 2.186,
            'H02' => 2.158,
            'H03' => 'bad',
            'H04' => 1.709,
            'H05' => 1.613,
            'H06' => 2.918,
            'H07' => 2.004,
            'H08' => 1.966,
            'H09' => 6.292,
            'H10' => 2.445,
            'H11' => 2.352,
            'H12' => 2.193,
            'H13' => 2.536,
            'H14' => 'bad',
            'H15' => 2.051 );

#my $result = JSA::QA::analyse_timeseries_rms( \%rms );
#print Dumper $result;

my %tsys = ( 'H00' => 261.49,
             'H01' => 228.43,
             'H02' => 307.60,
             'H03' => 'bad',
             'H04' => 211.21,
             'H05' => 226.36,
             'H06' => 315.17,
             'H07' => 276.93,
             'H08' => 238.73,
             'H09' => 378.75,
             'H10' => 332.89,
             'H11' => 310.12,
             'H12' => 299.83,
             'H13' => 260.26,
             'H14' => 'bad',
             'H15' => 288.94 );

# First, test tsysmax. All surveys should pass this.
$result = analyse_tsysmax( \%tsys );
foreach my $survey ( @surveys ) {
  ok( $result->{$survey}->pass, "Survey $survey successfully passed tsysmax test" );
}

# Now variance in Tsys. Only the GBS survey should fail.
$result = analyse_tsysvar( \%tsys );
foreach my $survey ( @surveys ) {
  if( $survey ne 'GBS' ) {
    ok( $result->{$survey}->pass, "Survey $survey successfully passed tsysvar test" );
  } else {
    ok( ! $result->{$survey}->pass, "Survey $survey successfully failed tsysvar test" );
  }
}

# Now the mixed test. Again, only the GBS survey should fail.
$result = analyse_tsys( \%tsys );
foreach my $survey ( @surveys ) {
  if( $survey ne 'GBS' ) {
    ok( $result->{$survey}->pass, "Survey $survey successfully passed tsysvar test" );
  } else {
    ok( ! $result->{$survey}->pass, "Survey $survey successfully failed tsysvar test" );
  }
}

# Test a single survey. This tests the option hash.
$result = analyse_tsys( \%tsys,
                        'survey' => 'GBS' );
ok( ! $result->{GBS}->pass, "Single survey test for GBS" );
ok( ! defined( $result->{SLS} ), "SLS survey test correctly not performed" );

# Now for the RMS test. Only the GBS should fail.
$result = analyse_timeseries_rms( \%rms );
foreach my $survey ( @surveys ) {
  if( $survey ne 'GBS' ) {
    ok( $result->{$survey}->pass, "Survey $survey successfully passed timeseries RMS test" );
  } else {
    ok( ! $result->{$survey}->pass, "Survey $survey successfully failed timeseries RMS test" );
  }
}
