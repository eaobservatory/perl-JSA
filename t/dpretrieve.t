#!perl

use strict;
use warnings;
use Test::More tests => 5;
use Test::Command;
use File::Spec;
use File::Temp;

my $com_path = File::Spec->catfile("blib","script","dpRetrieve");

# See if anything runs by using -h
# Note that we run the test using this perl and with the
# build library
my $test_run = Test::Command->new(cmd => "$^X -Mblib $com_path -h");

isa_ok($test_run, "Test::Command");
$test_run->exit_is_num(1, "Exit status from -h");

# get the full path to the test inputs file
my $inputs = File::Spec->catfile("testdata", "inputs.txt");

# Run it again with a temp directory as the output directory
my $tmpdir = File::Temp->newdir();

# we need to make sure that ORAC_DATA_IN is set
$ENV{ORAC_DATA_IN} = File::Spec->rel2abs("testdata");

$test_run = Test::Command->new(cmd => "$^X -Mblib $com_path --outdir $tmpdir --inputs $inputs");
isa_ok($test_run, "Test::Command");
$test_run->exit_is_num(0, "Exit status from copy");

# run it again but this time remove aaa.fit and replace it with
# a touched file
my $file = File::Spec->catdir($tmpdir, "aaa.fits");
unlink $file;
open my $fh, ">",$file;
close($fh);
$test_run->run();
$test_run->exit_isnt_num(0, "Should fail because file already exists and is different");
