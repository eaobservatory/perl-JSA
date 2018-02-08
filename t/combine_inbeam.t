use strict;

my $n_test; BEGIN {$n_test = 11;}
use Test::More tests => $n_test;

SKIP: {
    eval {
        require JAC::Setup; JAC::Setup->import(qw/omp archiving/);
        require JSA::EnterData;
    };

    skip 'OMP not present', $n_test if $@;

    my $dict = '/jac_sw/archiving/jcmt/import/data.dictionary';

    skip 'Data dictionary not present', $n_test unless -e $dict;

    my $enter = new JSA::EnterData(dict => $dict);

    isa_ok($enter, 'JSA::EnterData');

    is($enter->_combine_inbeam_values(
        'shutter',
        '',
        ''),
    '',
    'shutter first only');

    is($enter->_combine_inbeam_values(
        'shutter',
        'shutter',
        'shutter'),
    'shutter',
    'shutter in all');

    is($enter->_combine_inbeam_values(
        'shutter',
        'pol',
        'pol'),
    'pol',
    'shutter first then pol');

    is($enter->_combine_inbeam_values(
        'shutter pol',
        'pol',
        'pol'),
    'pol',
    'shutter first, pol in all');

    is($enter->_combine_inbeam_values(
        'shutter pol',
        '',
        ''),
    '',
    'shutter and pol in first only');

    is($enter->_combine_inbeam_values(
        'shutter',
        'pol',
        'fts2'),
    'fts2 pol',
    'shutter in first, then pol/fts2');

    is($enter->_combine_inbeam_values(
        'shutter a b c',
        'd f h',
        'e g i'),
    'd e f g h i',
    'shutter in first, then multiple');

    is($enter->_combine_inbeam_values(
        'shutter a b c',
        'shutter d f h',
        'shutter e g i'),
    'a b c d e f g h i shutter',
    'shutter in all with multiple');

    is($enter->_combine_inbeam_values(
        'a b c',
        'd f h',
        'e g i'),
    'a b c d e f g h i',
    'no shutter, multiple values');

    is($enter->_combine_inbeam_values(
        'fts2',
        'fts2',
        'fts2'),
    'fts2',
    'no shutter, fts2 in all');
}
