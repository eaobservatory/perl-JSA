#!perl

use Test::More tests => 3;

require_ok( "JSA::Files" );

my $uri = "ad:JCMT/jcmth20071215_00044_02_cube001_obs_000";
my $file = "jcmth20071215_00044_02_cube001_obs_000.fits";

is( JSA::Files::uri_to_file($uri), $file, "URI to file" );
is( JSA::Files::file_to_uri($file), $uri, "File to URI" );
