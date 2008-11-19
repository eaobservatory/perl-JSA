#!perl

use Test::More tests => 19;
use Data::Dumper;

my @surveys = qw/ NGS Telescope GBS SLS /;
my $config = "testdata/default.ini";

use_ok( "JSA::QA" );

# Set up a QA object.
my $qa = new JSA::QA( file => $config );

isa_ok( $qa, 'JSA::QA' );

my %rms = ( 'H00' => 1.977,
            'H01' => 2.186,
            'H02' => 2.158,
            'H03' => 'bad',
            'H04' => 1.709,
            'H05' => 1.613,
            'H06' => 2.718,
            'H07' => 2.004,
            'H08' => 1.966,
            'H09' => 6.292,
            'H10' => 2.445,
            'H11' => 2.352,
            'H12' => 2.193,
            'H13' => 2.536,
            'H14' => 'bad',
            'H15' => 2.051 );

my $result = $qa->analyse_timeseries_rms( \%rms );
isa_ok( $result, 'JSA::QA::Result' );
is( $result->pass, 0, 'Default fail on RMS test' );

$result = $qa->analyse_timeseries_rms( \%rms,
                                          survey => 'GBS' );
is( $result->pass, 0, 'GBS fails RMS test' );

$result = $qa->analyse_timeseries_rms( \%rms,
                                       survey => 'GBS',
                                       iterate => 1 );
is( $result->pass, 1, 'GBS passes after high-RMS receptors are removed' );

my %tsys = ( 'H00' => 261.49,
             'H01' => 228.43,
             'H02' => 307.60,
             'H03' => 'bad',
             'H04' => 211.21,
             'H05' => 226.36,
             'H06' => 315.17,
             'H07' => 276.93,
             'H08' => 238.73,
             'H09' => 678.75,
             'H10' => 332.89,
             'H11' => 310.12,
             'H12' => 299.83,
             'H13' => 260.26,
             'H14' => 'bad',
             'H15' => 288.94 );

$result = $qa->analyse_tsysmax( \%tsys );
isa_ok( $result, 'JSA::QA::Result' );
is( $result->pass, 1, "default tsysmax test passed" );

foreach my $survey ( @surveys ) {
  $result = $qa->analyse_tsysmax( \%tsys, 'survey' => $survey );
  ok( $result->pass, "Survey $survey successfully passed tsysmax test" );
}

$result = $qa->analyse_tsysvar( \%tsys );
isa_ok( $result, 'JSA::QA::Result' );
is( $result->pass, 1, "default tsysvar test passed" );

# Now variance in Tsys. Only the GBS survey should fail.
foreach my $survey ( @surveys ) {
  $result = $qa->analyse_tsysvar( \%tsys, 'survey' => $survey );
  is( $result->pass, 1, "Survey $survey successfully passed tsysvar test" );
}

$result = $qa->analyse_tsys( \%tsys );
is( $result->pass, 1, "default tsys test passed" );
foreach my $survey ( @surveys ) {
  $result = $qa->analyse_tsys( \%tsys, 'survey' => $survey );
  is( $result->pass, 1, "Survey $survey successfully passed tsys test" );
}
print Dumper $result;
