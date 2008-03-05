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

use JSA::Command qw/ run_command /;
use JSA::Error qw/ :try /;
use JSA::Files qw/ looks_like_drfile looks_like_cadcfile drfilename_to_cadc dissect_drfile
                 construct_rawfile looks_like_rawfile /;

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
  my ($stdout, $stderr, $exstat) =  run_command( {nothrow => 1}, @args );

  # strip error messages from stdout and place in stderr
  my (@out, @errors);
  @errors = @$stderr if @$stderr;
  for my $l (@$stdout) {
    if ($l =~ /^\s*\!/) {
      push(@errors, $l);
    } else {
      push(@out, $l);
    }
  }

  throw JSA::Error::BadExec( "Error running Starlink command $args[0] - status = $exstat.".(@errors ? " Errors:\n". join("\n",@errors) : "")."\n" )
    if $exstat != 0;

  return (\@out, \@errors, $exstat);
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

If the parent refers to a product that is not to be archived,
it will be removed from the provenance hierarchy and the new parents
will be processed. If they are also not to be archived, the process
will repeat until a root ancestor is found. We do not simply remove all
invalid parents but try to retain as much provenance information as possible
by removing only those entries required to give us a valid parent.

The allowed product names can be provided as a second argument

  update_parent_prov( $file, @products );

If no list is provided all products are assumed to be allowed.

=cut

sub prov_update_parent_path {
  my $file = shift;
  my @products = @_;

  # map to a hash
  my %prod = map { $_ => undef } @products;

  # first see if this is a valid NDF
  looks_like_drfile($file)
    or JSA::Error::BadFile->throw( "File '$file' does not look like it came from the DR");

  # Starlink status
  my $status = &NDF::SAI__OK;

  # open the file
  err_begin($status);
  ndf_begin();
  ndf_open( &NDF::DAT__ROOT(), $file, "UPDATE","OLD",my $indf, my $place,$status );

  # Get the 0th provenance entry
  my @parind = _get_prov_parents( $indf, 0, $status );

  # now go through the parents
  if ($status == &NDF::SAI__OK) {

    # find valid product in hierarchy
    my @validated;
    my @rejected;
    for my $i (@parind) {
      print "Checking parent $i\n";
      my ($ok, $rej) = _check_parent_product( $indf, $i, \%prod, $status );
      push(@validated, @$ok);
      push(@rejected, @$rej);
    }

    # Remove the rejected parents (in reverse order)
    for my $i (sort { $b <=> $a} @rejected) {
      print "Removing $i\n";
      ndg_rmprv( $indf, $i, $status);
    }

    print "Validated: ". join(" ", @validated)."\n";

    # Get the parent indices again. These should all be valid
    @parind = _get_prov_parents( $indf, 0, $status );
    print "After removal: ". join(" ",@parind)."\n";

    # Now go through each of the valid parents
    for my $i (@parind) {
      ndg_gtprv( $indf, $i, my $provloc, $status );

      # See if we have parents (useful for later)
      dat_there( $provloc, "PARENTS", my $haspar, $status );

      # Find Path locator
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

        # We need the header
        my $hdr = eval { Astro::FITS::Header::NDF->new(File => $path) };

        # We need to know if there is a product. There are two ways of doing this.
        # 1. Look at the filename.
        # 2. Read the PRODUCT header
        # Reconstructing the filename will be tricky without a header but see how far we can get
        $path =~ s/_0001_raw001/_raw001/;
        my $product;
        if (defined $hdr) {
          $product = $hdr->value("PRODUCT");
        } else {
          my @parts = dissect_drfile( $path );
          # raw is special so is not a real product
          if (defined $parts[5] && $parts[5] ne 'raw') {
            $product = $parts[5];
          }
        }

        # if there is no product it is a raw file so will not be
        # a FITS CADC filename.
        my $newpath;
        if (!$product || !$haspar) {
          # do nothing if this looks raw
          if (!looks_like_rawfile($path)) {
            # Recreate the raw file name
            if (defined $hdr) {
              $newpath = construct_rawfile( $hdr );
            } else {
              # This hack does not work for multi-subsystem hybrids
              # since we lose the subscan number
              $newpath = $path;
              $newpath =~ s/_[a-z]+(\d+)/_0$1/;
            }
          }
        } else {
          # We need the ASN_TYPE from this parent to correctly make the file name
          my $asntype = (defined $hdr ? $hdr->value("ASN_TYPE") : undef);

          # if we do not have a value we need to hope it's an
          # "obs" product
          $newpath = drfilename_to_cadc( $path,
                                         (defined $asntype ?
                                          (ASN_TYPE => $asntype) : ()));
        }

        if (defined $newpath) {
          # may need to resize
          if (length($newpath) > $pathlen) {
            # erases and create
            dat_annul($pathloc, $status);
            dat_erase($provloc, "PATH", $status);
            dat_new0c($provloc, "PATH", length($newpath), $status );
            dat_find( $provloc, "PATH", $pathloc, $status );
          }

          # modify the value in the HDS structure
          dat_put0c( $pathloc, $newpath, $status );

          # put it back in the file
          ndg_mdprv( $indf, $i, $provloc, $status );
        }
      }
      dat_annul( $provloc, $status );

    }
  } # no parents

  # close the ndf and free locators
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

=begin PRIVATE

=head1 PRIVATE FUNCTIONS

=over 4

=item B<_get_prov_parents>

Obtain the parent indices given the NDF identified and index of a provenance item.

  @indices = _get_prov_parents( $indf, 0, $status );

Returns empty list if there are no further parents. Uses inherited status.

=cut

sub _get_prov_parents {
  my $indf = shift;
  my $index = shift;

  # Note that we do not use a lexical for status since we want to
  # emulate the interface used for the NDF module
  return () if $_[0] != &NDF::SAI__OK;
  
  # Get the 0th provenance entry
  ndg_gtprv( $indf, $index, my $provloc, $_[0] );

  # get the parent indices
  dat_there( $provloc, "PARENTS", my $haspar, $_[0]);

  my @parind;
  if ($haspar) {
    cmp_size( $provloc, 'PARENTS',my $size, $_[0] );
    cmp_getvi( $provloc, 'PARENTS', $size, @parind, my $el, $_[0] );
  }
  dat_annul( $provloc, $_[0]);
  return @parind;
}

=item B<_check_parent_product>

Get the product for this parent and compare it with the allowed list.
If it does not match the allowed product name, the parent of that item
is checked until a match is found.

  ($ok, $rej) = _check_parent_product( $indf, $index, \%productlist, $status);

Returns the results as references to arrays. The first is an array of indices
that have valid products. The second is an array of indices that were checked and
rejected.

If the current parent index is okay it will be the only value stored in the first
array and the second array will be empty.

=cut

sub _check_parent_product {
  my $indf = shift;
  my $index = shift;
  my $prod = shift;

  # Note that we do not use a lexical for status since we want to
  # emulate the interface used for the NDF module
  return () if $_[0] != &NDF::SAI__OK;

  # first need to get the provenance information
  ndg_gtprv( $indf, $index, my $provloc, $_[0] );

  # get the PATH
  cmp_get0c( $provloc, "PATH", my $path, $_[0]);

  # clean up
  dat_annul( $provloc, $_[0]);
  print "PATH=$path $_[0]\n";
  my @rejected;
  my @isok;

  # now test it. If the parent looks like a CADC file already
  # then we assume that it is okay
  if ($_[0] == &NDF::SAI__OK) {
    # The path stored in the file lacks the .sdf
    $path .= ".sdf" unless $path =~ /\.sdf$/;

    print "HELLo\n";
    if (looks_like_cadcfile($path)) {
      # assume that if the provenance already includes CADC form
      # that this file is okay
      print "Looks like CADCFILE\n";
      @isok = ($index);

    } elsif (looks_like_drfile($path)) {
      # Rather than read the file header (which may cost a lot of time)
      # parse the filename
      print "Looks like DR\n";
      my @parts = dissect_drfile( $path );

      if (exists $prod->{$parts[5]}) {
        # we are good
        print "Product match\n";
        @isok = ($index);
      }
    } else {
      print "Strange match\n";
    }

    # if we have got here with an empty index list we need to look in the parent
    if (!@isok) {
      my @parents = _get_prov_parents( $indf, $index, $_[0] );
      if (@parents) {
        push(@rejected, $index);
        for my $i (@parents) {
          my ($ok, $rej) = _check_parent_product($indf, $i, $prod, $_[0] );
          push(@isok, @$ok);
          push(@rejected, @$rej);
        }
      } else {
        # if there are no more parents we have to assume that this is a valid
        # parent.
        print "No parents for $index\n";
        push(@isok, $index);
      }
    }

  }

  return (\@isok, \@rejected);
}

=back

=end PRIVATE

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

