#!perl

use Test::More tests => 1;
use Data::Dumper;

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

my $result = JSA::QA::analyse_timeseries_rms( \%rms );
print Dumper $result;

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

$result = JSA::QA::analyse_tsysmax( \%tsys );
print Dumper $result;
$result = JSA::QA::analyse_tsysvar( \%tsys );
print Dumper $result;
