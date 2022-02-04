package JSA::WrapDR;

=head1 NAME

JSA::WrapDR - Data reduction wrapping subroutines

=cut

use strict;
use warnings;

use File::Copy;
use File::Spec;
use File::Temp qw/tempfile/;

use JSA::Command qw/run_command/;
use JSA::Files qw/looks_like_cadcfile scan_dir/;
use JSA::Headers qw/get_orac_instrument/;
use JSA::Logging qw/log_message log_command/;

use parent qw/Exporter/;
our @EXPORT_OK = qw/prepare_environment
                    retrieve_data determine_instrument
                    run_pipeline capture_products
                    clean_directory_final clean_directory_pre_capture
                    log_listing read_file_list/;

our $VERSION = '0.03';

=head1 SUBROUTINES

=over 4

=item prepare_environment

Prepare environment for pipeline.

=cut

sub prepare_environment {
    # Set ADAM_USER to a temp directory
    my $adamuser = File::Temp->newdir();
    $ENV{ADAM_USER} = "$adamuser";
}

=item retrieve_data

Copy the files locally.

=cut

sub retrieve_data {
    my ($inputs, $show_output, $outdir) = @_;

    # output directory defaulting
    $outdir = File::Spec->curdir
        unless defined $outdir;

    # absolute output directory name for error messages
    my $absdir = File::Spec->rel2abs($outdir);

    # open the file and read all the lines
    my $file = $inputs;
    die "retrieve_data: No file supplied so unable to retrieve any data files"
        unless defined $file;

    open (my $fh, "<", $file)
        or die "Could not open supplied filename '$file': $!";

    my @lines = <$fh>;

    close($fh) or die "Could not close file '$file': $!";

    for my $line (@lines) {
        my $fullpath = _parse_retrieve_line($line);
        next unless defined $fullpath;

        # see if that path exists
        if (-e $fullpath) {
            # split into a directory and a filename
            my ($vol, $dir, $file) = File::Spec->splitpath($fullpath);

            # output location
            $file = File::Spec->catdir($outdir, $file);

            # see if it exists already in the current dir
            if (-e $file) {
                # We first have to decide whether it is the same
                # file or note
                my @statcurr = stat($file);
                my @statnew  = stat($fullpath);

                my $same = 1;
                for my $i (0..$#statcurr) {
                    if ($statcurr[$i] != $statnew[$i]) {
                        $same = 0;
                        last;
                    }
                }
                if (!$same) {
                    die "Unable to retrieve file '$fullpath' because a file named '$file' already exists in the retrieve directory ($absdir) and it is different";
                }

                # we know they are the same file
                if (-l $file) {
                    # remove the link and remake it
                    if (unlink $file) {
                        symlink $fullpath, $file
                            or die "Could not make soft link to '$file' in retrieve directory ($absdir)";

                        log_message("Re-making soft link for '$file'")
                            if $show_output;
                    }
                }
                else {
                    # it is a real file locally and it is the same so we touch
                    # it to allow the wrapper script to know that it is required
                    my $atime = time;
                    my $mtime = $atime;

                    utime($atime, $mtime, $file)
                        or die "Error touching file '$file' in current directory ($absdir)";

                    log_message("Touching pre-existing file '$file'")
                        if $show_output;
                }
            }
            else {
                # make a soft link
                symlink $fullpath, $file
                    or log_message("Could not make soft link to '$file' in current directory ($absdir)");
                log_message("Making new soft link for file '$file'")
                    if $show_output;
            }
        }
        else {
            log_message("File '$fullpath' does not seem to exist");
        }
    }
}

# Converts a line retrieved from an "inputs" file into a full path
# Recognizes full path to file /xxx/yy
# For path-less strings prepends $ORAC_DATA_IN or current dir if not set
# Comments (# xxx) are removed

sub _parse_retrieve_line {
    my $path = shift;
    chomp($path);
    $path =~ s/\#.*//;
    return unless $path =~ /\w/;

    # see if we need to attach a directory. We do this if the
    # file is relative.
    unless (File::Spec->file_name_is_absolute($path)) {
        # no directory so we guess ORAC_DATA_IN, else it will have
        # to be current working directory. We check in ORAC_DATA_IN
        if (exists $ENV{ORAC_DATA_IN} &&
                defined $ENV{ORAC_DATA_IN} &&
                -d $ENV{ORAC_DATA_IN}) {
            my $newpath = File::Spec->catdir($ENV{ORAC_DATA_IN}, $path);
            $path = $newpath if -e $newpath;
        }
    }

    return $path;
}

=item determine_instrument

Determine the ORAC_INSTRUMENT from the supplied headers.

=cut

sub determine_instrument {
    my ($FITS) = @_;

    my %instruments;
    for my $f (keys %$FITS) {
        my $oa = get_orac_instrument($FITS->{$f});
        die "Unable to determine ORAC_INSTRUMENT for file '$f'\n"
        unless defined $oa;
        $instruments{$oa} ++;
    }

    if (keys %instruments > 1) {
        die "Can not process files from multiple instruments (" .
            join(",",keys %instruments).")\n";
    }

    # get the single instrument
    my ($oracinst) = keys %instruments;
    return $oracinst;
}

=item run_pipeline

Run the ORAC-DR or PICARD pipeline.

    run_pipeline($useoracdr, $oracinst, $indir,
                 $outdir, $files_or_ut, $drparameters, \%options);

C<$files_or_ut> is either a reference to a list of files, or in
dpRetrieve-skipping mode, a string containing the UT date.

=cut

sub run_pipeline {
    my ($useoracdr, $oracinst, $indir, $outdir, $files_or_ut,
        $drparameters, $options) = @_;

    # Configure shared environment variables
    $ENV{'ORAC_DATA_IN'} = (defined $indir ? $indir : $outdir);
    $ENV{ORAC_DATA_OUT} = $outdir;

    # ORAC_CAL_ROOT
    if (!exists $ENV{ORAC_CAL_ROOT}) {
        $ENV{ORAC_CAL_ROOT} = File::Spec->catdir($ENV{ORAC_DIR}, File::Spec->updir,
                                                 "cal");
    }

    # ORAC_PERL5LIB is only set if required
    if (!exists $ENV{ORAC_PERL5LIB}) {
        $ENV{ORAC_PERL5LIB} = File::Spec->catdir( $ENV{ORAC_DIR}, "lib", "perl5" );
    }

    # Create $tmpfile in subroutine scope so that it is not automatically
    # cleared up before we get around to running the data reduction command.
    my $tmpfile;
    my $preprocess_files = undef;

    my @drcommand;
    if (defined $options->{'drcommand'}) {
        my $command = $options->{'drcommand'};
        log_message("Using DR command: $command\n");

        # Write file list as for ORAC-DR.
        $tmpfile = File::Temp->new() or die "Could not create temporary file";
        print $tmpfile "$_\n" foreach @$files_or_ut;
        close $tmpfile or die "Error closing temporary file handle";

        @drcommand = (
            $command,
            '--id', $options->{'id'},
            '--inputs', "$tmpfile",
        );

        push @drcommand, '--fileversion', $options->{'version'}
            if defined $options->{'version'};

        push @drcommand, '--transdir', $options->{'transdir'}
            if ($options->{'persist'} and (defined $options->{'transdir'}));

        push(@drcommand, split /\s+/, $drparameters) if defined $drparameters;
    }
    elsif ($useoracdr) {
        # Check for pre-processing mode.
        my $pipename = 'ORAC-DR';
        if (exists $options->{'preprocess'} and $options->{'preprocess'}) {
            $preprocess_files = new File::Temp();
            $preprocess_files->close();
            $pipename = 'WESLEY';
        }

        log_message(sprintf "Using %s\n", $pipename);

        # Instrument
        $ENV{ORAC_INSTRUMENT} = $oracinst;

        # ORAC_DATA_CAL.
        unless (exists($ENV{'ORAC_DATA_CAL'})) {
            $ENV{'ORAC_DATA_CAL'} = File::Spec->catdir($ENV{'ORAC_CAL_ROOT'},
                                                       lc($ENV{'ORAC_INSTRUMENT'}));
        }

        @drcommand = ($^X,
                      File::Spec->catfile($ENV{ORAC_DIR}, "bin", "oracdr"),
                      "-nodisplay",
                      "-log","hs",
                      "-verbose",
                     );

        push @drcommand, '-batch' if $options->{'batch'};

        # In "skip dpRetrieve" mode there is no list of files, but a UT
        # date instead.
        unless (ref $files_or_ut) {
            push @drcommand, "-ut", $files_or_ut, "-skip", "-from", "1",
                "-loop", ($oracinst =~ /^scuba$/i ? "inf" : "flag");
        }
        else {
            # We now have to write the files to a text file suitable for
            # ORAC-DR to read with the -file option
            $tmpfile = File::Temp->new()
                or die "Could not create a temporary file";
            print $tmpfile "$_\n" for @$files_or_ut;
            close($tmpfile) or die "Error closing temp file handle";

            push @drcommand, "-loop", "file", "-file", "$tmpfile"
        }

        # Add the DR parameters.
        push(@drcommand, split /\s+/, $drparameters) if defined $drparameters;

        # Add Wesley options if preprocessing.
        if (defined $preprocess_files) {
            push @drcommand, '-preprocess',
                '-recpars=WESLEY_FILE_LIST=' . $preprocess_files->filename();
        }
    }
    else {
        # We are going to need a ^file option in PICARD
        log_message("Using PICARD\n");

        # PICARD needs a recipe name so we need to abort if we do not have one
        die "Processing reduced data requires the use of the -drparameters option\n"
            unless (defined $drparameters);

        # PICARD only uses a recipe name from drparameters
        @drcommand = ($^X,
                      File::Spec->catfile($ENV{ORAC_DIR}, "bin", "picard"),
                      "-nodisplay",
                      "-log","hs",
                      "-verbose",
                      (split /\s+/, $drparameters),
                      @$files_or_ut);

    }

    # Run the DR command.
    # We need to decide whether a partial execution of the DR should result in
    # file ingestion.
    log_message("Running: " . join(" ", @drcommand));
    my ($drstdout, $drstderr, $drstatus) = run_command({nothrow => 1},
                                                       @drcommand);
    map {s/\e\[(?:\d{1,2};?)+m//g;} @$drstdout;
    map {s/\e\[(?:\d{1,2};?)+m//g;} @$drstderr;
    log_message("\n*** Last ten lines of STDOUT:\n");
    my $upper = ($#$drstdout > 9 ? 10 : $#$drstdout + 1);
    log_message(join "\n", grep {defined} @$drstdout[-$upper .. -1]);
    log_message("\n*** All output from STDERR:\n");
    log_message(join "\n", @$drstderr);
    die "Non-zero pipeline exit status: $drstatus" if $drstatus;

    # Read file list if preprocessing.
    if (defined $preprocess_files) {
        return read_file_list($preprocess_files->filename());
    }
}

=item capture_products

Capture products.

=cut

sub capture_products {
    my ($persist, $transdir, $show_output) = @_;

    log_message("*** capturing products");

    if (defined $transdir) {
        $transdir = File::Spec->rel2abs($transdir);
    }
    else {
        die 'capture_products: transdir not specified';
    }

    # Regex
    my $regexp = qr/^(?:jcmt.*\.fits|jcmt.*\.png)$/;

    # Sort out output directory
    if ($persist) {
        unless (-d $transdir) {
            my $updir = _parentdir($transdir);
            if (-d $updir) {
                mkdir($transdir)
                    or die "Could not create directory '$transdir' required to receive data files";
            }
            else {
                die "$transdir does not exist and neither does its parent. Can not copy data";
            }
        }
    }

    # Scan the directory for plausible fits or png files
    my %files = scan_dir($regexp);

    for my $f (sort keys %files) {
        if (looks_like_cadcfile($f)) {
            if ($persist) {
                # Copy the file into a temporary file in the current directory,
                # then rename it.
                my ($fh, $filename) = tempfile();
                copy($f, $filename) or die "copy from $f to $filename failed: $!";
                move($filename, File::Spec->catfile($transdir, $f))
                    or die "move from $filename to " . File::Spec->catfile($transdir, $f) . " failed: $!";

                log_message("Copied $f to " . File::Spec->catfile($transdir, $f))
                    if $show_output;

            }
            else {
                log_message("Would persist file $f")
                    if $show_output;
            }
        }
    }
}

# Given a directory path, returns the path of the parent.
#  $parent = _parentdir( $dir );

sub _parentdir {
    my $path = shift;
    my @dirs = File::Spec->splitdir($path);
    pop(@dirs);
    my $updir = File::Spec->catdir(@dirs);
    return $updir;
}

=item clean_directory_pre_capture

Clean up for CADC if required.

=cut

sub clean_directory_pre_capture {
    my ($outdir, $existing_files, $filter) = @_;

    # Do not want to cleanup files that were present before the
    # command began
    my %post_all = scan_dir(qr/.*/);

    for my $file (keys %post_all) {
        unless (exists $existing_files->{$file}) {
            # was not previously here
            unless ($filter->($file)) {
                # does not look like a CADC file
                # do not check status
                unlink File::Spec->catfile($outdir, $file);
            }
        }
    }
}

=item clean_directory_final

Clean up the output directory.

=cut

sub clean_directory_final {
    my ($outdir, $existing_files) = @_;

    # Clean files one at a time making sure we do not remove
    # a file that was already present.
    opendir (my $DH, $outdir) or die "Could not open the output directory!";

    for my $f (readdir($DH)) {
        unless (exists $existing_files->{$f}) {
            # Use unlink and do not check status
            # since there is not a lot we can do at this point.
            my $path = File::Spec->catfile($outdir, $f);
            unlink $path;
        }
    }
}

=item log_listing

Print a debugging listing.

=cut

sub log_listing {
    my ($name, $directory) = @_;

    log_message('DEBUG LISTING BEGIN: ' . $name);
    log_message($_) foreach `ls $directory`;
    log_message('DEBUG LISTING END');
}

=item read_file_list

Read an ORAC-DR style file list.

This can be used to read file lists written by Wesley.  Comments
starting with # are ignored.  However no checks for duplicate files
are performed.

Returns a reference to an array of files, or undef if the listing
could not be read.

=cut

sub read_file_list {
    my $filename = shift;

    my $fh = new IO::File($filename, 'r');
    return undef unless defined $fh;

    my @files = ();
    foreach my $f (<$fh>) {
        # Same filtering as ORAC::Core::orac_parse_files:
        chomp $f;
        $f =~ s/\#.*//;             # comments
        $f =~ s/^\s+//;             # leading whitespace
        $f =~ s/\s+$//;             # trailing whitespace
        next unless $f =~ /\w/;
        push @files, $f;
    }

    $fh->close();
    return \@files;
}

1;

__END__

=back

=head1 AUTHORS

Extracted from jsawrapdr which was written by:

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>

=head1 COPYRIGHT

Copyright (C) 2008-2011 Science and Technology Facilities Council.
All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful,but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place,Suite 330, Boston, MA  02111-1307, USA

=cut
