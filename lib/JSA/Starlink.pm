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
use File::Basename;
use warnings::register;

use Proc::SafeExec;

# Need NDG support in NDF
use NDF 1.47;

# find out where starlink is located
use Starlink::Config qw/ :override /;

use JSA::Command qw/ run_command /;
use JSA::Error qw/ :try /;
use JSA::Files qw/ looks_like_drfile looks_like_cadcfile drfilename_to_cadc dissect_drfile
                   construct_rawfile looks_like_rawfile can_send_to_cadc can_send_to_cadc_guess /;
use JSA::Headers qw/ read_header /;

use Exporter 'import';
our @EXPORT_OK = qw/ check_star_env
                     run_star_command
                     prov_update_parent_path
                     set_wcs_attribs
                   /;

our $DEBUG = 0;

our %PARENT_PRODUCT_CACHE = ();

=head1 FUNCTIONS

=over 4

=item B<check_star_env>

Check that the Starlink environment is okay. Without arguments
simply checks that $STARLINK_DIR is set and pointing to a
directory.

  check_star_env();

The first argument (optional) refers to the name of an application
whose corresponding APPNAME_DIR environment variable should exist.
If that variable is not defined Starlink::Config is used to locate
it. If it can not be found throws a C<JSA::Error::BadEnv> exception.
If it is successfully located APPNAME_DIR will be set appropriately.

  check_star_env( $appname, $command );

The second optional argument refers to a command within that
application which should exist in the $APPNAME_DIR directory.

Throws a C<JSA::Error::BadEnv> if there is something wrong with
the environment.

=cut

sub check_star_env {
  my $appname = shift;
  my $command = shift;

  # if APPNAME_DIR environment variable is defined and that directory exists
  # we do not do anything further

  # Note that Starlink::Config will use $STARLINK_DIR itself since we have override
  # enabled.
  throw JSA::Error::BadEnv( "Do not know where the Starlink software is installed. You may need to set \$STARLINK_DIR.") 
    unless (exists $StarConfig{"Star"} && -d $StarConfig{"Star"});

  if (defined $appname) {

    # Work out the environment variable name
    my $env = uc($appname);
    $env .= "_DIR" unless $env =~ /_DIR$/;

    # if that envrinment variable exists and the directory exists we are okay
    if (exists $ENV{$env} && -d $ENV{$env}) {
	# everything is okay
    } else {
	# Try to find the requested directory.
	my $dir = $appname;
	$dir =~ s/_dir$//i;
	$dir = lc( $dir );

	my $testdir = File::Spec->catfile($StarConfig{"Star_Bin"}, $dir);

	throw JSA::Error::BadEnv("$dir directory could not be found in Starlink software directory tree." )
	    unless -d $testdir;

	# if we get here then that directory is okay so set the environment
	$ENV{$env} = $testdir;
    }

    # check for the command
    if (defined $command) {

      my $app = File::Spec->catfile( $ENV{$env}, $command );

      throw JSA::Error::BadEnv("Command '$app' does not seem to exist")
        unless -e $app;

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

=item B<prov_update_parent_path>

Rename the provenance entries in the supplied file to match the CADC
filenaming convention rather than the CADC naming scheme.

  prov_update_parent_path( $file );

Note that this command only works if the parent file actually exists,
since in many cases the ASN_TYPE header is required to determine the
CADC filename. "Obs" products can be special cased (and are
special-cased in the drfilename_to_cadc routine).

Only the immediate parents are processed.

If the parent refers to a product that is not to be archived, it will
be removed from the provenance hierarchy and the new parents will be
processed. If they are also not to be archived, the process will
repeat until a root ancestor is found. We do not simply remove all
invalid parents but try to retain as much provenance information as
possible by removing only those entries required to give us a valid
parent.

=cut

sub prov_update_parent_path {
  my $file = shift;

  # first see if this is a valid NDF
  looks_like_drfile($file)
    or JSA::Error::BadFile->throw( "File '$file' does not look like it came from the DR");

  print "Updating parent provenance path for file $file\n" if $DEBUG;

  # Grab the base directory.
  my( $filename, $basedir, $suffix ) = fileparse( $file );

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
    my %checked = ();
    for my $i (@parind) {
      next if( $checked{$i} );
      print "Checking parent $i\n" if $DEBUG;
      my ($ok, $rej) = _check_parent_product( $indf, $i, $file, $status );
      push(@validated, @$ok);
      push(@rejected, @$rej);
      $checked{$i}++;
    }

    # Remove the rejected parents (in reverse order)
    my %seen = ();
    for my $i (sort { $b <=> $a } grep { ! $seen{$_} ++ } @rejected) {
      print "Removing $i\n" if $DEBUG;
      ndg_rmprv( $indf, $i, $status);
    }

    print "Validated: ". join(" ", @validated)."\n" if $DEBUG;
    print "Removed: " . join( " ", @rejected ) . "\n" if $DEBUG;

    # Get the parent indices again. These should all be valid
    @parind = _get_prov_parents( $indf, 0, $status );
    print "After removal: ". join(" ",@parind)."\n" if $DEBUG;

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

        # Check to see if this file exists. If it doesn't, we'll check in the same directory as the original file.
        if ( ! -e $path ) {
           my $parent_filename = fileparse( $path );
           $path = File::Spec->catfile( $basedir, $parent_filename );
        }

        # We need the header
        my $hdr = read_header( $path );

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
    my $err = err_flush_to_string( $status );
    err_end($status);
    JSA::Error::Starlink->throw( $err );
  }
  err_end($status);

  return;
}

=item B<set_wcs_attribs>

Set appropriate WCS attributes for an NDF.

  set_wcs_attribs( $file );

The following WCS attributes are set:

 o StdOfRest = BARY
 o System(1) = FK5
 o System(3) = FREQ

Takes one argument, the file for which the WCS attributes are to be
set.

Returns undef.

=cut

sub set_wcs_attribs {
  my $file = shift;

  check_star_env( "KAPPA", "wcsattrib" );

  # Read the WCS from the file to see whether a specframe is present
  my $status = &NDF::SAI__OK();
  err_begin($status);
  ndf_begin();

  # Retrieve the WCS from the NDF.
  ndf_find(&NDF::DAT__ROOT(), $file, my $indf, $status);
  my $wcs = ndfGtwcs( $indf, $status );
  ndf_annul($indf, $status);
  my $errstr;
  if ($status != &NDF::SAI__OK()) {
    $errstr = &NDF::err_flush_to_string( $status );
  }
  ndf_end($status);
  err_end($status);
  throw JSA::Error::FatalError("Error reading WCS from file $file: $errstr")
    if defined $errstr;

  # See if we have a SpecFrame
  my $template = Starlink::AST::SpecFrame->new( "MaxAxes=7" );
  my $spf = $wcs->FindFrame( $template, " " );

  # Form argument string
  my @argstr = qw/ System(1)=FK5 /;
  if (defined $spf) {
    push(@argstr, qw/ System(3)=FREQ StdOfRest=BARY / );
  }

  print "Forcing attributes of file $file to ".join( " ",@argstr)."\n"
    if $DEBUG;

  # Now set the attributes
  my @args = ( File::Spec->catfile( $ENV{KAPPA_DIR}, "wcsattrib" ),
               "NDF=$file",
               "MODE=MSet",
               "SETTING='".join(",",@argstr)."'",
             );

  run_star_command( @args );
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

# Note that $status is never shifted off the argument stack.
# This is done so that the value will be changed in the caller
# version of $status. A lexical variable $status would not propagate
# badness to the caller.

sub _get_prov_parents {
  my $indf = shift;
  my $index = shift;

  # Note that we do not use a lexical for status since we want to
  # emulate the interface used for the NDF module
  return () if $_[0] != &NDF::SAI__OK;

  # Get the 0th provenance entry
  ndg_gtprv( $indf, $index, my $provloc, $_[0] );

  print "Retrieved 0th provenance entry.\n" if $DEBUG;

  # get the parent indices
  dat_there( $provloc, "PARENTS", my $haspar, $_[0]);

  my @parind;
  if ($haspar) {
    cmp_size( $provloc, 'PARENTS',my $size, $_[0] );
    cmp_getvi( $provloc, 'PARENTS', $size, @parind, my $el, $_[0] );
  }
  dat_annul( $provloc, $_[0]);

  my %seen = ();
  my @uniq_parind = grep { ! $seen{$_} ++ } @parind;

  return @uniq_parind;
}

=item B<_check_parent_product>

Get the product for this parent and compare it with the allowed list.
If it does not match the allowed product name, the parent of that item
is checked until a match is found.

  ($ok, $rej) = _check_parent_product( $indf, $index, $file, $status );

Returns the results as references to arrays. The first is an array of indices
that have valid products. The second is an array of indices that were checked and
rejected.

If the current parent index is okay it will be the only value stored in the first
array and the second array will be empty.

=cut

# Note that $status is never shifted off the argument stack.
# This is done so that the value will be changed in the caller
# version of $status. A lexical variable $status would not propagate
# badness to the caller.

sub _check_parent_product {
  my $indf = shift;
  my $index = shift;
  my $file = shift;

  if( defined( $PARENT_PRODUCT_CACHE{$file}{$index}{'ok'} ) &&
      defined( $PARENT_PRODUCT_CACHE{$file}{$index}{'rej'} ) ) {
    return( $PARENT_PRODUCT_CACHE{$file}{$index}{'ok'},
            $PARENT_PRODUCT_CACHE{$file}{$index}{'rej'} );
  }

  # Note that we do not use a lexical for status since we want to
  # emulate the interface used for the NDF module
  return () if $_[0] != &NDF::SAI__OK;

  # first need to get the provenance information
  ndg_gtprv( $indf, $index, my $provloc, $_[0] );

  # get the PATH
  cmp_get0c( $provloc, "PATH", my $path, $_[0]);

  # clean up
  dat_annul( $provloc, $_[0]);
  print "PATH=$path $_[0]\n" if $DEBUG;
  my @rejected;
  my @isok;
  print "Testing index $index\n" if $DEBUG;
  # now test it. If the parent looks like a CADC file already
  # then we assume that it is okay
  if ($_[0] == &NDF::SAI__OK) {
    # The path stored in the file lacks the .sdf
    $path .= ".sdf" unless $path =~ /\.sdf$/;

    if (looks_like_cadcfile($path)) {
      # assume that if the provenance already includes CADC form
      # that this file is okay
      print "Looks like CADCFILE\n" if $DEBUG;
      @isok = ($index);

    } elsif (looks_like_drfile($path)) {
      # Reading the header may take a lot longer than parsing the
      # filename but for now we do that since that is required
      # if we do not wish to reimplement the logic in can_send_to_cadc.
      print "Looks like DR ($path)\n" if $DEBUG;

      # in some cases intermediate files have been deleted even
      # though they match the dr file name test. We use a quick
      # test to see if they are close to being relevant and if they
      # are relevant we do an additional test with the header
      if (can_send_to_cadc_guess( $path ) ) {

        # Open up the header, send it to can_send_to_cadc() to find
        # out if this file is a suitable one to send to CADC.
        my $hdr = read_header( $path );
        if (!defined $hdr) {
          if ($_[0] == &NDF::SAI__OK()) {
            $_[0] = &NDF::SAI__ERROR();
            err_rep( " ", "Unable to read FITS header from $path", $_[0] );
          }
        }

        if ( can_send_to_cadc( $hdr ) ) {
          # we are good
          print "Product match\n" if $DEBUG;
          @isok = ($index);
        }
      }
    } elsif ( looks_like_rawfile( $path ) ) {
      print "Looks like raw\n" if $DEBUG;
      @isok = ($index);
    } else {
      print "Strange match\n" if $DEBUG;
    }

    # if we have got here with an empty index list we need to look in the parent
    if (!@isok) {
      my @parents = _get_prov_parents( $indf, $index, $_[0] );
      if (@parents) {
        push(@rejected, $index);
        for my $i (@parents) {
          my ($ok, $rej) = _check_parent_product( $indf, $i, $file, $_[0] );
          push(@isok, @$ok);
          push(@rejected, @$rej);
        }
      } else {
        # if there are no more parents we have to assume that this is a valid
        # parent.
        print "No parents for $index\n" if $DEBUG;
        push(@isok, $index);
      }
    }

  }

  my %seen = ();
  my @isok_uniq = grep { ! $seen{$_} ++ } @isok;
  %seen = ();
  my @rejected_uniq = grep { ! $seen{$_} ++ } @rejected;

  $PARENT_PRODUCT_CACHE{$file}{$index}{'ok'} = \@isok_uniq;
  $PARENT_PRODUCT_CACHE{$file}{$index}{'rej'} = \@rejected_uniq;

  return (\@isok_uniq, \@rejected_uniq);
}

=back

=end PRIVATE

=head1 AUTHORS

Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>,

=head1 COPYRIGHT

Copyright (C) 2008-2009 Science and Technology Facilities Council.
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

