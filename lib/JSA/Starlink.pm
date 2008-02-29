package JSA::Starlink;

=head1 NAME

JSA::Starlink - Helper functions to support the Starlink environment.

=head1 SYNOPSIS

  use JSA::Starlink;
  check_star_env( "CONVERT", "ndf2fits" );

=head1 DESCRIPTION

This module provides helper function that are useful when working
in the Starlink environment.

=cut

use strict;
use Carp;
use warnings;
use File::Spec;
use warnings::register;

use Proc::SafeExec;

# Need NDG support in NDF
use NDF 1.47;
use Astro::FITS::Header::NDF;

use JSA::Error qw/ :try /;
use JSA::Files qw/ looks_like_drfile looks_like_cadcfile drfilename_to_cadc /;

use Exporter 'import';
our @EXPORT_OK = qw/ check_star_env
                     run_star_command
                     prov_update_parent_path
                   /;

=head1 FUNCTIONS

=over 4

=item B<check_star_env>

Check that the Starlink environment is okay. Without arguments
simply checks that $STARLINK_DIR is set and pointing to a
directory.

  check_star_env();

The first argument (optional) refers to the name of an application
whose environment variable should be tested.

  check_star_env( $appname, $command );

The second optional argument refers to a command within that application
which should exist in $appname_DIR directory.

Throws a C<JSA::Error::BadEnv> if there is something wrong with
the environment.

=cut

sub check_star_env {
  my $appname = shift;
  my $command = shift;

  throw JSA::Error::BadEnv( "Do not know where the Starlink software is installed. Please set \$STARLINK_DIR.") 
    unless (exists $ENV{STARLINK_DIR} && -d $ENV{STARLINK_DIR});

  if (defined $appname) {
    my $env = $appname;
    $env .= "_DIR" unless $appname =~ /_DIR$/;

    throw JSA::Error::BadEnv("$env environment variable is either not set or the directory does not exist")
      unless (exists $ENV{$env} && -d $ENV{$env});

    # check for the command
    if (defined $command) {

      throw JSA::Error::BadEnv("Command '$command' does not seem to exist in $ENV{$env} directory")
        unless -e File::Spec->catfile($ENV{$env}, $command );

    }

  }

}

=item B<run_star_command>

Run a Starlink command using the supplied arguments.

  run_star_command( $command, @args );

The command should include the full path to the command.
Throws C<JSA::Error::BadExec> if the command fails.

Note that C<JSA::Command> provides the C<run_command> function
for running non-Starlink commands.

=cut

sub run_star_command {
  my @args = @_;

  # Make sure we get an exit status
  local $ENV{ADAM_EXIT} = 1;

  # Note that errors are written to STDOUT not STDERR.

  my $conv = Proc::SafeExec->new( { exec => \@args,
                                    stdout => "new"
                                  } );

  $conv->wait;

  my $exstat = $conv->exit_status >> 8;
  throw JSA::Error::BadExec( "Error running command $args[0] - status = $exstat" )
    if $exstat != 0;
  return 1;
}

=item C<prov_update_parent_path>

Rename the provenance entries in the supplied file to match the
CADC filenaming convention rather than the CADC naming scheme.

  update_parent_prov( $file );

Note that this command only works if the parent file actually
exists, since in many cases the ASN_TYPE header is required
to determine the CADC filename. "Obs" products can be special
cased (and are special-cased in the drfilename_to_cadc routine).

Only the immediate parents are processed.

=cut

sub prov_update_parent_path {
  my $file = shift;

  # first see if this is a valid NDF
  looks_like_drfile($file)
    or JSA::Error::BadFile->throw( "File '$file' does not look like it came from the DR");

  # Starlink status
  my $status = &NDF::SAI__OK;

  # open the file
  err_begin($status);
  ndf_begin();
  ndf_open( &NDF::DAT__ROOT, $file, "UPDATE","OLD",my $indf, my $place,$status );

  # Get the 0th provenance entry
  ndg_gtprv( $indf, 0, my $provloc, $status );

  # get the parent indices
  dat_there( $provloc, "PARENTS", my $haspar, $status);

  if ($haspar) {
    cmp_size( $provloc, 'PARENTS',my $size, $status );
    my @parind;
    cmp_getvi( $provloc, 'PARENTS', $size, @parind, my $el, $status );

    # now go through the parents
    if ($status == &NDF::SAI__OK) {
      for my $i (@parind) {
        ndg_gtprv( $indf, $i, my $provloc, $status );

        # Find Parents locator
        dat_find( $provloc, "PATH", my $pathloc, $status );

        # Ask its length
        dat_clen( $pathloc, my $pathlen, $status);

        # get the PATH
        dat_get0c( $pathloc, my $path, $status );

        # proceed if status is good and the path does not already
        # look correct
        if ($status == &NDF::SAI__OK && !looks_like_cadcfile($path)) {

          # The path stored in the file lacks the .sdf
          $path .= ".sdf" unless $path =~ /\.sdf$/;

          # We need ASN_TYPE header from this parent file
          my $hdr = Astro::FITS::Header::NDF->new(File => $path);
          my $asntype = $hdr->value("ASN_TYPE");

          # if we do not have a value we need to hope it's an
          # "obs" product
          my $cadc = drfilename_to_cadc( $path,
                                         (defined $asntype ?
                                          (ASN_TYPE => $asntype) : ()));

          if (defined $cadc) {
            # may need to resize
            if (length($cadc) > $pathlen) {
              # erases and create
              dat_annul($pathloc, $status);
              dat_erase($provloc, "PATH", $status);
              dat_new0c($provloc, "PATH", length($cadc), $status );
              dat_find( $provloc, "PATH", $pathloc, $status );
            }

            # modify the value in the HDS structure
            dat_put0c( $pathloc, $cadc, $status );

            # put it back in the file
            ndg_mdprv( $indf, $i, $provloc, $status );
          }
        }

      }
    }
  } # no parents

  # close the ndf and free locators
  dat_annul( $provloc, $status );
  ndf_annul( $indf, $status);
  ndf_end( $status );

  if ($status != &NDF::SAI__OK) {
    my @errs = err_flush_to_string( $status );
    err_end($status);
    JSA::Error::Starlink->throw( join("\n",@errs) );
  }
  err_end($status);

  return;
}

=back

=head1 AUTHORS

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>,

=head1 COPYRIGHT

Copyright (C) 2008 Science and Technology Facilities Council.
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

1;

