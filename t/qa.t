#!perl

use Test::More tests => 24;
use Data::Dumper;

my @surveys = qw/NGS GBS SLS/;
my $config = "testdata/default.ini";

use_ok("JSA::QA");

# Set up a QA object.
my $qa = new JSA::QA(file => $config);

isa_ok($qa, 'JSA::QA');

my %rms = (
    'H00' => 1.977,
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
    'H15' => 2.051,
);

my $result = $qa->analyse_timeseries_rms(\%rms);
isa_ok($result, 'JSA::QA::Result');
is($result->pass, 0, 'Default fail on RMS test');

$result = $qa->analyse_timeseries_rms(\%rms, survey => 'GBS');
is($result->pass, 0, 'GBS fails RMS test');

$result = $qa->analyse_timeseries_rms(\%rms, survey => 'GBS', iterate => 1);
is($result->pass, 1, 'GBS passes after high-RMS receptors are removed');

my %tsys = (
    'H00' => 261.49,
    'H01' => 228.43,
    'H02' => 307.60,
    'H03' => 'bad',
    'H04' => 211.21,
    'H05' => 226.36,
    'H06' => 315.17,
    'H07' => 276.93,
    'H08' => 338.73,
    'H09' => 608.75,
    'H10' => 332.89,
    'H11' => 310.12,
    'H12' => 299.83,
    'H13' => 260.26,
    'H14' => 'bad',
    'H15' => 288.94,
);

$result = $qa->analyse_tsysmax(\%tsys);
isa_ok($result, 'JSA::QA::Result');
is($result->pass, 1, "default tsysmax test passed");

foreach my $survey (@surveys) {
    $result = $qa->analyse_tsysmax(\%tsys, 'survey' => $survey);
    ok($result->pass, "Survey $survey successfully passed tsysmax test");
}

$result = $qa->analyse_tsysvar(\%tsys);
isa_ok($result, 'JSA::QA::Result');
is($result->pass, 1, "default tsysvar test passed");

# Now variance in Tsys. Only the GBS survey should fail.
foreach my $survey (@surveys) {
    $result = $qa->analyse_tsysvar(\%tsys, 'survey' => $survey);

    if ($survey eq 'GBS') {
        is($result->pass, 0, "Survey $survey successfully failed tsysvar test");
    }
    else {
        is($result->pass, 1, "Survey $survey successfully passed tsysvar test");
    }
}

# Test GBS C18O. Should pass the Tsys variance test.
$result = $qa->analyse_tsysvar(\%tsys, 'survey' => 'GBS', molecule => 'C18O');
is($result->pass, 1, "Survey GBS, molecule C18O successfully passed tsysvar test");

# Test absolute Tsys numbers.
$result = $qa->analyse_tsys(\%tsys);
is($result->pass, 1, "default tsys test passed");

# ...and for the surveys. Again, only GBS should fail.
foreach my $survey (@surveys) {
    $result = $qa->analyse_tsys(\%tsys, 'survey' => $survey);
    if( $survey eq 'GBS' ) {
        is($result->pass, 0, "Survey $survey successfully failed tsys test");
    }
    else {
        is($result->pass, 1, "Survey $survey successfully passed tsys test");
    }
}

# Run Tsys test again, this time using iteration to eliminate
# high-Tsys receptors one by one until pass.
$result = $qa->analyse_tsys(\%tsys, survey => 'GBS', iterate => 1 );
is($result->pass, 1, "GBS passes after high-Tsys receptors are removed");

# Now try some RxA sample data.
%tsys = ('A' => 790.12);

# This should pass.
$result = $qa->analyse_tsysvar(\%tsys,
                               survey => 'telescope',
                               frequency => 265.88618);
is($result->pass, 1, "RxA by frequency passes Tsys test");

# This should fail.
$result = $qa->analyse_tsysvar(\%tsys,
                               survey => 'telescope',
                               molecule => 'CO21',
                               frequency => 230.538);
is($result->pass, 0, "RxA for CO 2-1 fails Tsys test");
