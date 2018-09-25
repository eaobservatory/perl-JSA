use strict;

my $n_test; BEGIN {$n_test = 1 + 30 + 3;}
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

    # Test "_find_header" method.
    my %header = (
        HEADER1 => 1,
        HEADER6 => undef,
        HEADER7 => 0,
        HEADER8 => 'aardvark',
        HEADER0 => 'x',

        SUBHEADERS => [
            {
                HEADER2 => 10,
            },
            {
                HEADER2 => 11,
                HEADER0 => 'y',
            },
            {
                HEADER3 => undef,
                HEADER4 => 1,
                HEADER5 => 0,
                HEADER9 => 'zebra',
                HEADER0 => 'z',
            },
        ],
    );

    ok($enter->_find_header(headers => \%header, name => 'HEADER1'));
    ok($enter->_find_header(headers => \%header, name => 'HEADER2'));
    ok($enter->_find_header(headers => \%header, name => 'HEADER6'));
    ok(! $enter->_find_header(headers => \%header, name => 'NOHEADER'));

    ok($enter->_find_header(headers => \%header, name => 'HEADER1', test => 'true'));
    ok($enter->_find_header(headers => \%header, name => 'HEADER4', test => 'true'));
    ok(! $enter->_find_header(headers => \%header, name => 'HEADER5', test => 'true'));
    ok(! $enter->_find_header(headers => \%header, name => 'HEADER7', test => 'true'));

    ok($enter->_find_header(headers => \%header, name => 'HEADER1', test => 'defined'));
    ok($enter->_find_header(headers => \%header, name => 'HEADER4', test => 'defined'));
    ok($enter->_find_header(headers => \%header, name => 'HEADER5', test => 'defined'));
    ok($enter->_find_header(headers => \%header, name => 'HEADER7', test => 'defined'));
    ok(! $enter->_find_header(headers => \%header, name => 'HEADER3', test => 'defined'));
    ok(! $enter->_find_header(headers => \%header, name => 'HEADER6', test => 'defined'));

    is_deeply([sort $enter->_find_header(headers => \%header, name => 'HEADER1', value => 1)],
              [1]);
    is_deeply([sort $enter->_find_header(headers => \%header, name => 'HEADER2', value => 1)],
              [10, 11]);
    is_deeply([sort $enter->_find_header(headers => \%header, name => 'HEADER0', value => 1)],
              # Existing implementation only returns primary header value if found there.
              # [qw/x y z/]);
              [qw/x/]);

    is(scalar $enter->_find_header(headers => \%header, name => 'HEADER8', value => 1),
       'aardvark');
    is(scalar $enter->_find_header(headers => \%header, name => 'HEADER2', value => 1),
       10);

    ok($enter->_find_header(headers => \%header, name => 'HEADER8', value_regex => qr/^a/));
    ok(! $enter->_find_header(headers => \%header, name => 'HEADER8', value_regex => qr/^z/));

    ok(! $enter->_find_header(headers => \%header, name => 'HEADER9', value_regex => qr/^a/));
    ok($enter->_find_header(headers => \%header, name => 'HEADER9', value_regex => qr/^z/));

    %header = (
        HEADER1 => 10,
        HEADER2 => 0,
    );

    ok($enter->_find_header(headers => \%header, name => 'HEADER1'));
    ok(! $enter->_find_header(headers => \%header, name => 'NOHEADER'));
    ok($enter->_find_header(headers => \%header, name => 'HEADER1', test => 'true'));
    ok(! $enter->_find_header(headers => \%header, name => 'HEADER2', test => 'true'));
    ok($enter->_find_header(headers => \%header, name => 'HEADER1', test => 'defined'));
    ok($enter->_find_header(headers => \%header, name => 'HEADER2', test => 'defined'));
    is(scalar $enter->_find_header(headers => \%header, name => 'HEADER1', value => 1),
       10);

    # Test the "skip_calc_radec" method.
    ok(! $enter->skip_calc_radec(headers => {}));
    ok(! $enter->skip_calc_radec(headers => {OBS_TYPE => 'pointing'}));
    ok($enter->skip_calc_radec(headers => {OBS_TYPE => 'skydip'}));
}
