#!perl

use strict;
use warnings;
use Test::More tests => 35;

require_ok( "JSA::Files" );

my @obsfiles = qw/
    a20071102_00019_01_raw001.sdf
    a20071102_00014_01_cube001.sdf
    s20071102_00014_450_reduced001.sdf
    h20001022_00105_16_rsp.sdf
    19991011_0024_resw.sdf
    19991011_0025_lon_reb.sdf
    19991011_0020_p20_pht.sdf
/;

for my $f (@obsfiles) {
    ok(JSA::Files::looks_like_drfile($f), "Is it a DR file?");
    my $cadc = JSA::Files::drfilename_to_cadc($f);
    ok(defined $cadc, "Converted to CADC format");
    print "# $f -> $cadc\n";
    is(JSA::Files::cadc_to_drfilename($cadc),$f, "Convert back to original");
}

# Groups (we need an association type)

my @grpfiles = qw/
    19991011_grp_0018_pht_long.sdf
    19991011_grp_0019_pht_p1350.sdf
    ga20080605_39_1_reduced001.sdf
/;

for my $f (@grpfiles) {
    ok(JSA::Files::looks_like_drfile($f), "Is it a DR file?");
    my $cadc = JSA::Files::drfilename_to_cadc($f, ASN_TYPE => "night");
    ok(defined $cadc, "Converted to CADC format");
    print "# $f -> $cadc\n";
    is(JSA::Files::cadc_to_drfilename($cadc), $f, "Convert back to original");
}


# Raw files
my @rawfiles = qw/
    19990916_dem_0030.sdf
    19990920_dem_0031_1.sdf
    a20080510_00036_02_0001.sdf
    s8a20090602_00042_0002.sdf
/;

for my $f (@rawfiles) {
    ok(JSA::Files::looks_like_rawfile($f), "$f looks like a raw files");
}
