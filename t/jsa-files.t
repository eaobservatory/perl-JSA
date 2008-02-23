#!perl

use strict;
use warnings;
use Test::More tests => 15;

require_ok( "JSA::Files" );

my $uri = "ad:JCMT/jcmth20071215_00044_02_cube001_obs_000";
my $file = "jcmth20071215_00044_02_cube001_obs_000.fits";

is( JSA::Files::uri_to_file($uri), $file, "URI to file" );
is( JSA::Files::file_to_uri($file), $uri, "File to URI" );

my @obsfiles = qw/
                a20071102_00019_01_raw001.sdf
                a20071102_00014_01_cube001.sdf
                s20071102_00014_450_reduced001.sdf
                h20001022_00105_16_rsp.sdf
              /;

for my $f (@obsfiles) {
  ok( JSA::Files::looks_like_drfile($f), "Is it a DR file?");
  my $cadc = JSA::Files::drfilename_to_cadc( $f );
  ok( defined $cadc, "Converted to CADC format" );
  print "# $f -> $cadc\n";
  is( JSA::Files::cadc_to_drfilename($cadc),$f, "Convert back to original");
}
