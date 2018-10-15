use strict;

my $n_test; BEGIN {$n_test = 1 + 4 + 4 + 4;}
use Test::More tests => $n_test;
use Test::Number::Delta;

SKIP: {
    eval {
        require JAC::Setup; JAC::Setup->import(qw/omp archiving/);
        require JSA::EnterData::SCUBA2;
    };

    skip 'OMP not present', $n_test if $@;

    my $dict = '/jac_sw/archiving/jcmt/import/data.dictionary';

    skip 'Data dictionary not present', $n_test unless -e $dict;

    my $enter = new JSA::EnterData::SCUBA2(dict => $dict);

    isa_ok($enter, 'JSA::EnterData::SCUBA2');

    # Test get_total_int_time method.

    my %header = (
        OBS_TYPE => 'focus',
        SUBHEADERS => [
            {
                'NSUBSCAN' => 1,
                'SEQ_TYPE' => 'FASTFLAT',
                'SUBARRAY' => 's8a',
                'INT_TIME' => 30.0,
            },
            {
                'NSUBSCAN' => 1,
                'SEQ_TYPE' => 'FASTFLAT',
                'SUBARRAY' => 's8b',
                'INT_TIME' => 30.0,
            },
        ],
    );

    delta_ok($enter->get_total_int_time(\%header), 0.0);

    push @{$header{'SUBHEADERS'}}, {
                'NSUBSCAN' => 2,
                'SEQ_TYPE' => 'focus',
                'SUBARRAY' => 's8a',
                'INT_TIME' => 30.0,
            };

    delta_ok($enter->get_total_int_time(\%header), 30.0);

    push @{$header{'SUBHEADERS'}}, {
                'NSUBSCAN' => 2,
                'SEQ_TYPE' => 'focus',
                'SUBARRAY' => 's8b',
                'INT_TIME' => 30.0,
            };

    delta_ok($enter->get_total_int_time(\%header), 60.0);

    push @{$header{'SUBHEADERS'}}, {
                'NSUBSCAN' => 3,
                'SEQ_TYPE' => 'focus',
                'SUBARRAY' => 's8a',
                'INT_TIME' => 30.0,
            };

    delta_ok($enter->get_total_int_time(\%header), 90.0);

    # Test get_subarray_count method.

    my @headers = {
            FILTER     => '450',
            SUBARRAY_a => 1,
    };

    is($enter->get_subarray_count(\@headers), 1);

    push @headers, {
            FILTER     => '450',
            SUBARRAY_a => 1,
            SUBARRAY_b => 1,
    };

    is($enter->get_subarray_count(\@headers), 2);

    push @headers, {
            FILTER     => '450',
            SUBARRAY_a => 1,
            SUBARRAY_c => 0,
    };

    is($enter->get_subarray_count(\@headers), 2);

    push @headers, {
            FILTER     => '850',
            SUBARRAY_a => 1,
    };

    is($enter->get_subarray_count(\@headers), 3);

    # Test combine_int_time method.

    @headers = {
            filter     => '850',
            subarray_a => 1,
            subarray_b => 1,
            int_time   => 30.0,
    };

    delta_ok($enter->combine_int_time(\@headers), 30.0);

    push @headers, {
            filter     => '850',
            subarray_a => 1,
            int_time   => 30.0,
    };

    delta_ok($enter->combine_int_time(\@headers), 45.0);

    push @headers, {
            filter     => '850',
            subarray_b => 1,
            int_time   => 30.0,
    };

    delta_ok($enter->combine_int_time(\@headers), 60.0);

    push @headers, {
            filter     => '450',
            subarray_a => 1,
            subarray_b => 1,
            int_time   => 60.0,
    };

    delta_ok($enter->combine_int_time(\@headers), 60.0);
}
