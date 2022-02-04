#!perl

use strict;
use warnings;
use Test::More tests => 3;
use File::Spec;
use File::Temp;

use_ok('JSA::WrapDR');

# get the full path to the test inputs file
my $inputs = File::Spec->catfile("testdata", "inputs.txt");

# Run it again with a temp directory as the output directory
my $tmpdir = File::Temp->newdir();

# we need to make sure that ORAC_DATA_IN is set
$ENV{ORAC_DATA_IN} = File::Spec->rel2abs("testdata");

my $result = eval {
    JSA::WrapDR::retrieve_data($inputs, 0, $tmpdir);
    1;
};
ok(defined $result, "Exit status from copy");

# run it again but this time remove aaa.fit and replace it with
# a touched file
my $file = File::Spec->catdir($tmpdir, "aaa.fits");
unlink $file;
open my $fh, ">",$file;
close($fh);

$result = eval {
    JSA::WrapDR::retrieve_data($inputs, 0, $tmpdir);
    1;
};
ok((not defined $result), "Should fail because file already exists and is different");
