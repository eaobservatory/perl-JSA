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
use JSA::Headers qw/ read_wcs /;

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

  if ( $exstat != 0 ) {

    my $text =
      "Error running Starlink command $args[0] - status = $exstat."
      . ( @errors
          ? " Errors:\n" . join( "\n", @errors )
          : ''
        )
      . "\n" ;

    throw JSA::Error::StarlinkCommand( $text )
      if $exstat == 1;

    throw JSA::Error::BadExec( $text );
  }

  return (\@out, \@errors, $exstat);
}

=item B<prov_update_parent_path>

Rename the provenance entries in the supplied file to match the CADC
filenaming convention rather than the CADC naming scheme.

  prov_update_parent_path( $file, \&is_dr_file, \&is_archive_file,
                           \&check_file, \&convert_filename,
                           $strict_check );

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

This subroutine makes use of a number of callbacks which implement
the logic required to select and convert provenance entries.

=over 4

=item \&is_dr_file($file)

Determine if the file is a DR product.  Only used at the start of the
subroutine to reject files which are not DR products.

e.g. C<JSA::Files::looks_like_drfile>.

=item \&is_archive_file($file)

Determine if the file already has a CADC name.

e.g. C<JSA::Files::looks_like_cadcfile>.

=item \&check_file($path)

Determine whether a file should be included or not.

=item \&convert_filename($path, $basedir, $haspar)

Convert a filename to the corresponding CADC filename for
storage in the provenance.

=back

If the C<$strict_check> argument isn't given then provenance
entries which don't have any valid parents are retained.

=cut

sub prov_update_parent_path {
  my $file = shift;

  my $is_dr_file = shift;
  my $is_archive_file = shift;
  my $check_file = shift;
  my $convert_filename = shift;
  my $strict_check = shift;

  # first see if this is a valid NDF
  $is_dr_file->($file)
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

  # Read the provenance from the file
  my $prov = ndgReadProv( $indf, "", $status );

  # Get the 0th provenance entry
  my @parind = _get_prov_parents( $prov, 0, $status );

  # now go through the parents
  if ($status == &NDF::SAI__OK) {

    # find valid product in hierarchy
    my @validated;
    my @rejected;
    my %checked = ();
    for my $i (@parind) {
      next if( $checked{$i} );
      print "Checking parent $i\n" if $DEBUG;
      my ($ok, $rej) = _check_parent_product( $prov, $i, $file, $check_file,
                                              $strict_check, $status );
      last if $status != &NDF::SAI__OK;
      push(@validated, @$ok);
      push(@rejected, @$rej);
      $checked{$i}++;
    }

    # Remove the rejected parents (in reverse order)
    my %seen = ();
    my @toremove = sort { $b <=> $a } grep { ! $seen{$_} ++ } @rejected;
    print "Removing ". @toremove . " unused ancestors\n" if $DEBUG;
    $prov->RemoveProv( \@toremove, $status );

    print "Validated: ". join(" ", @validated)."\n" if $DEBUG;
    print "Removed ".@rejected.": " . join( " ", @rejected ) . "\n" if $DEBUG;
    print "Sent to remove: ".join( " ", @toremove). "\n" if $DEBUG;

    # Get the parent indices again. These should all be valid
    @parind = _get_prov_parents( $prov, 0, $status );
    print "After removal: ". join(" ",@parind)."\n" if $DEBUG;

    # Now go through each of the valid parents
    for my $i (@parind) {
      my $provkm = $prov->GetProv( $i, $status );

      # See if we have parents (useful for later)
      my $haspar = $provkm->MapHasKey( "PARENTS" );

      # get the PATH
      my $path = $provkm->MapGet0C( "PATH" );

      # proceed if status is good and the path does not already
      # look correct
      if ($status == &NDF::SAI__OK && !$is_archive_file->($path)) {

        # The path stored in the file lacks the .sdf
        $path .= ".sdf" unless $path =~ /\.sdf$/;

        my $newpath = $convert_filename->($path, $basedir, $haspar);

        if (defined $newpath) {
          # update the path in the keymap
          $provkm->MapPut0C( "PATH", $newpath, "" );

          # put it back in the provenance structure
          $prov->ModifyProv( $i, $provkm, $status );
        }
      }

    } # foreach @parind

    # write out the updated provenance structure
    $prov->WriteProv( $indf, 0, $status );

  } # status not ok

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
  my $wcs = read_wcs( $file );

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

  @indices = _get_prov_parents( $prov, 0, $status );

Returns empty list if there are no further parents. Uses inherited status.

=cut

# Note that $status is never shifted off the argument stack.
# This is done so that the value will be changed in the caller
# version of $status. A lexical variable $status would not propagate
# badness to the caller.

sub _get_prov_parents {
  my $prov = shift;
  my $index = shift;

  # Note that we do not use a lexical for status since we want to
  # emulate the interface used for the NDF module
  return () if $_[0] != &NDF::SAI__OK;

  # Get the 0th provenance entry
  my $km = $prov->GetProv( $index, $_[0] );

  print "Retrieved provenance entry #$index to retrieve parents.\n" if $DEBUG;
#  $km->Show();

  # get the parent indices
  my $haspar = $km->MapHasKey( "PARENTS" );

  my @parind;
  if ($haspar) {
    @parind = $km->MapGet1I( "PARENTS" );
  }

  my %seen = ();
  my @uniq_parind = grep { ! $seen{$_} ++ } @parind;
  print "With parent indices: (". join(",",@uniq_parind). ")\n" if $DEBUG;
  return @uniq_parind;
}

=item B<_check_parent_product>

Get the product for this parent and check whether it is allowed.
If it does not match the allowed product name, the parent of that item
is checked until a match is found.

  ($ok, $rej) = _check_parent_product( $prov, $index, $file,
                                       \&check_file, $strict_check, $status );

Returns the results as references to arrays. The first is an array of indices
that have valid products. The second is an array of indices that were checked and
rejected.

If the current parent index is okay it will be the only value stored in the first
array and the second array will be empty.

The subroutine reference C<\&check_file($path)> is used to check
files to see whether they are allowed or not.

=cut

# Note that $status is never shifted off the argument stack.
# This is done so that the value will be changed in the caller
# version of $status. A lexical variable $status would not propagate
# badness to the caller.

sub _check_parent_product {
  my $prov = shift;
  my $index = shift;
  my $file = shift;
  my $check_file = shift;
  my $strict_check = shift;

  # Note that we do not use a lexical for status since we want to
  # emulate the interface used for the NDF module
  return () if $_[0] != &NDF::SAI__OK;

  if( defined( $PARENT_PRODUCT_CACHE{$file}{$index}{'ok'} ) &&
      defined( $PARENT_PRODUCT_CACHE{$file}{$index}{'rej'} ) ) {
    return( $PARENT_PRODUCT_CACHE{$file}{$index}{'ok'},
            $PARENT_PRODUCT_CACHE{$file}{$index}{'rej'} );
  }

  # first need to get the provenance information
  my $provkm = $prov->GetProv( $index, $_[0] );

  # get the PATH
  my $path = $provkm->MapGet0C( "PATH" );

  # clean up
  print "_check_parent_product PATH $index=$path $_[0]\n" if $DEBUG;
  my @rejected;
  my @isok;
  print "Testing index $index\n" if $DEBUG;
#  $provkm->Show();
  # now test it. If the parent looks like a CADC file already
  # then we assume that it is okay
  if ($_[0] == &NDF::SAI__OK) {
    # The path stored in the file lacks the .sdf
    $path .= ".sdf" unless $path =~ /\.sdf$/;

    if( basename( $file ) eq basename( $path ) ) {
      print "Looks like original file ($file == $path)\n" if $DEBUG;
    } else {
      my $want_file = eval {$check_file->($path)};
      unless (defined $want_file) {
        if ($_[0] == &NDF::SAI__OK()) {
          $_[0] = &NDF::SAI__ERROR();
          err_rep( " ", $@, $_[0] );
          return ();
        }
      }
      @isok = ($index) if $want_file;
    }

    # if we have got here with an empty index list we need to look in the parent
    if (!@isok) {
      my @parents = _get_prov_parents( $prov, $index, $_[0] );
      if (@parents) {
        push(@rejected, $index);
        for my $i (@parents) {
          print "Checking parent $i\n" if $DEBUG;
          my ($ok, $rej) = _check_parent_product( $prov, $i, $file, $check_file,
                                                  $strict_check, $_[0] );
          return () unless $_[0] == &NDF::SAI__OK();
          push(@isok, @$ok);
          push(@rejected, @$rej);
        }
      } elsif ($strict_check) {
        # In strict-check mode, if no valid parents were found, reject this
        # provenance entry.
        print "No parents for $index -- rejected by strict check\n" if $DEBUG;
        push @rejected, $index;
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

