package JSA::WrapDR;

=head1 NAME

JSA::WrapDR - Data reduction wrapping subroutines

=cut

use strict;
use warnings;

use File::Spec;
use File::Temp;

use JSA::Command qw/run_command/;
use JSA::Files qw/looks_like_cadcfile scan_dir/;
use JSA::Logging qw/log_message log_command/;

use parent qw/Exporter/;
our @EXPORT_OK = qw/run_pipeline capture_products
                    clean_directory_final clean_directory_pre_capture/;

=head1 SUBROUTINES

=over 4

=item run_pipeline

Run the ORAC-DR or PICARD pipeline.

    run_pipeline($useoracdr, $oracinst, $indir,
                 $outdir, $files_or_ut, $drparameters);

C<$files_or_ut> is either a reference to a list of files, or in
dpRetrieve-skipping mode, a string containing the UT date.

=cut

sub run_pipeline {
  my ($useoracdr, $oracinst, $indir, $outdir, $files_or_ut,
      $drparameters) = @_;

  # We want normal messages from Starlink subsystem
  $ENV{MSG_FILTER} = "NORM" unless exists $ENV{MSG_FILTER};

  # Configure shared environment variables
  $ENV{'ORAC_DATA_IN'} = ( defined $indir ? $indir : $outdir );
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

  my @drcommand;
  if ($useoracdr) {
    log_message( "Using ORAC-DR\n" );

    # Instrument
    $ENV{ORAC_INSTRUMENT} = $oracinst;

    # ORAC_DATA_CAL.
    if( ! exists( $ENV{'ORAC_DATA_CAL'} ) ) {
      $ENV{'ORAC_DATA_CAL'} = File::Spec->catdir($ENV{'ORAC_CAL_ROOT'},
                                                 lc($ENV{'ORAC_INSTRUMENT'}));
    }

    @drcommand = ( $^X,
                 File::Spec->catfile($ENV{ORAC_DIR},"bin","oracdr"),
                 "-nodisplay",
                 "-log","hs",
                 "-verbose",
                 "-recsuffix", "ADV,CADC",
                 "-batch" );

    # In "skip dpRetrieve" mode there is no list of files, but a UT
    # date instead.
    unless (ref $files_or_ut) {
      push @drcommand, "-ut", $files_or_ut, "-skip", "-from", "1",
        "-loop", ($oracinst =~ /^scuba$/i ? "inf" : "flag");
    } else {
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

  } else {
    # We are going to need a ^file option in PICARD
    log_message( "Using PICARD\n" );

    # PICARD needs a recipe name so we need to abort if we do not have one
    die "Processing reduced data requires the use of the -parameters option\n"
      unless (defined $drparameters);

    # PICARD only uses a recipe name from drparameters
    @drcommand = ( $^X,
                 File::Spec->catfile($ENV{ORAC_DIR},"bin","picard"),
                 "-log","hs",
                 "-verbose",
                 ( split /\s+/, $drparameters ),
                 @$files_or_ut
               );

  }

  # Run the DR command.
  # We need to decide whether a partial execution of the DR should result in
  # file ingestion.
  log_message( "Running: " . join( " ", @drcommand ) );
  my ( $drstdout, $drstderr, $drstatus ) = run_command( { nothrow => 1 },
                                                        @drcommand);
  map { s/\e\[(?:\d{1,2};?)+m//g; } @$drstdout;
  map { s/\e\[(?:\d{1,2};?)+m//g; } @$drstderr;
  log_message( "\n*** Last ten lines of STDOUT:\n" );
  my $upper = ( $#$drstdout > 9 ? 10 : $#$drstdout+1 );
  log_message( join "\n", grep { defined } @$drstdout[-$upper..-1] );
  log_message( "\n*** All output from STDERR:\n" );
  log_message( join "\n", @$drstderr );
  die "Non-zero pipeline exit status: $drstatus" if $drstatus;
}

=item capture_products

Call dpCapture with the correct arguments.

=cut

sub capture_products {
  my ($id, $persist, $transdir, $archive, $show_output) = @_;
  my @args;
  if( $persist ) {
    push @args, "--persist";
  }
  if( defined( $transdir ) ) {
    push @args, "--transdir", $transdir;
  }
  push @args, '--archive=' . $archive;

  log_message( "\n*** calling dpCapture --id=$id " . ( join " ", @args ) . "\n" );

  my( $dpcstdout, $dpcstderr, $dpcstatus ) = run_command("dpCapture", "--id=$id", @args);
  log_command( "dpCapture", $dpcstdout, $dpcstderr ) if $show_output;
}

=item clean_directory_pre_capture

Clean up for CADC if required.

=cut

sub clean_directory_pre_capture {
  my ($outdir, $existing_files) = @_;

  # Do not want to cleanup files that were present before the
  # command began
  my %post_all = scan_dir( qr/.*/ );

  for my $file (keys %post_all) {
    if (! exists $existing_files->{$file}) {
      # was not previously here
      if (! looks_like_cadcfile( $file ) ) {
        # does not look like a CADC file
        # do not check status
        unlink File::Spec->catfile( $outdir, $file );
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
    if (!exists $existing_files->{$f}) {
      # Use unlink and do not check status
      # since there is not a lot we can do at this point.
      my $path = File::Spec->catfile($outdir, $f);
      unlink $path;
    }
  }
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
