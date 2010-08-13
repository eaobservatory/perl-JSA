package JSA::CADC_DP;

=head1 NAME

JSA::CADC_DP - Connect to CADC data processing system and submit jobs.

=head1 SYNOPSIS

use JSA::CADC_DP qw/ connect_to_cadcdp disconnect_from_cadcdp create_recipe_instance /;
my $dbh = connect_to_cadcdp;
create_recipe_instance( $dbh, \@members );
disconnect_from_cadcdp( $dbh );

=head1 DESCRIPTION

C<JSA::CADC_DP> sets up connections to the CADC data processing database, allowing one to submit processing jobs.

=cut

use warnings;
use strict;

use Carp;
use DBD::Sybase;
use Math::BigInt;
use JSA::Error;

use OMP::Config;

use Exporter 'import';
our @EXPORT_OK = qw/ connect_to_cadcdp disconnect_from_cadcdp
                     create_recipe_instance /;

our $VERBOSE = 0;
our $DEBUG = 0;

# Define connection information.
my $READDATABASE = "jcmtmd";
my $WRITEDATABASE = "data_proc";
my $DBSERVER = "CADC_ASE";
my $AD = "JCMT";
my $DBUSER = OMP::Config->getData( 'cadc_dp.user' );
my $DBPSWD = OMP::Config->getData( 'cadc_dp.password' );

=head1 DATA PROCESSING CONSTANTS

The following constants are available to define a data processing recipe.

 CADC_DPREC_8G  - 8GB 64bit system
 CADC_DPREC_16G - 16GB 64bit system

You can assume they are numerical constants where a higher value indicates
more resources are required.

=cut

# We use a constant integer indicating a resource
# level so that we can compare whether a particular observation needs
# more resources than previously thought. We then need to map that
# constant to a dprecipe tag known to the processing system
use constant CADC_DPREC_8G => 8;
use constant CADC_DPREC_16G => 16;

=head1 FUNCTIONS

=over 4

=item B<connect_to_cadcdp>

Create a connection to the CADC data processing database.

  my $dbh = connect_to_cadcdp;

=cut

sub connect_to_cadcdp {
  my $dbh = DBI->connect( "dbi:Sybase:server=$DBSERVER;database=$WRITEDATABASE",
                          "$DBUSER", "$DBPSWD",
                          { PrintError => 0,
                            RaiseError => 0,
                            AutoCommit => 0,
                          } ) or &cadc_dberror;
  if (!$dbh) {
    throw JSA::Error::CADCDB( "Could not connect to database as $DBUSER" );
  }

  return $dbh;
}

=item B<disconnect_from_cadcdp>

Disconnect from the CADC data processing database.

  disconnect_from_cadcdp( $dbh );

=cut

sub disconnect_from_cadcdp {
  my $dbh = shift;
  $dbh->disconnect;
}

sub cadc_dberror {
  my ( $msg ) = @_;
  throw JSA::Error::CADCDB( "DB Problem: $DBI::errstr" );
}

=item B<create_recipe_instance>

Add a list of requests for processing.

  $recipe_id = create_recipe_instance( $dbh, \@members, \%opts );

This function takes two parameters: the first being the database
handle as returned from connect_to_cadcdp, and the second being an
array reference pointing to an array of URIs to be processed.

This function takes one optional parameter: a hash reference with the
following optional keys:

 - tag: String to associate with this recipe (usually the ASN_ID)
 - mode: Grouping mode ("night", "project", "public")
 - project: A recipe parameters override. Assumes a recpars-$project.ini
            recipe file exists in ORAC-DR.
 - priority: Relative priority of job as an integer. Default
             priority will be -1. Range is between -500 and 0.
 - queue: Name of CADC processing queue (can be undef)
 - drparams: Additional options for jsawrapdr pipeline. Would usually be
          a recipe name.
 - dprecipe: Constant indicating which CADC processing recipe to use to reduce the data.
          Default will be CADC_DPREC_8G.

This function returns the recipe instance ID on success, or undef for failure.

=cut

sub create_recipe_instance {
  my $dbh = shift;
  my $MEMBERSREF = shift;

  my $options = shift;

  my $mode = $options->{'mode'};
  my $project = defined( $options->{'project'} ) ? uc( $options->{'project'} ) : undef;
  my $queue = ( defined $options->{queue} ? uc( $options->{queue} ) : undef );
  my $drparams = ( defined $options->{drparams} ? $options->{drparams} : "" );
  my $tag = $options->{tag};

  # Default to medium priority. Note that this differs to the CADC default of -500.
  my $priority = -1;
  if (exists $options->{priority} && defined $options->{priority} ) {
    my $override = $options->{priority};
    if ($override < -500) {
      $priority = -500;
    } elsif ($override > 0) {
      $priority = 0;
    } else {
      $priority = $override;
    }
  }

  my $sql;

  print "VERBOSE: create a row in dp_recipe_instance\n" if $VERBOSE;

  ###############################################
  # First, find the recipe_id for jsawrapdr
  ###############################################

  # Default to 8G
  my $dp_recipe_const = ( defined $options->{dprecipe} ? $options->{dprecipe} :
                          CADC_DPREC_8G );

  # Need to map the processing constant to something that can be looked for
  # in the database table
  my %DPRECMAP = (# Do not use => as the key is the value of the constant
                  CADC_DPREC_8G, "8G",
                  CADC_DPREC_16G, "16G",
               );

  # Work out the corresponding string
  throw JSA::Error::CADCDB( "DP recipe constant does not match a known value" )
    unless exists $DPRECMAP{$dp_recipe_const};
  my $dp_recipe_tag = $DPRECMAP{$dp_recipe_const};

  $sql = <<ENDRECIPEID;
select recipe_id
   from $WRITEDATABASE..dp_recipe
   where script_name="jsawrapdr" and description like '%$dp_recipe_tag'
ENDRECIPEID

  my $dp_recipe_id = queryBinaryValue( $dbh, $sql );
  throw JSA::Error::CADCDB( "Cannot retrieve good recipe_id from dp_recipe" )
    unless $dp_recipe_id;

  ###############################################
  # Use the maximum current value of recipe_instance_id in
  # dp_recipe_instance to generate a "new" recipe_instance_id
  ###############################################

  my $dp_recipe_instance_bigint = queryMaxBinaryValue( $dbh, "dp_recipe_instance",
                                                       "recipe_instance_id") +1;
  my $dp_recipe_instance_id = bigintstr($dp_recipe_instance_bigint);

  ###############################################
  # Use the maximum cuurent value of input_id in
  # dp_file_input to generate a "new" input_id
  ###############################################

  my $dp_file_input_bigint  = queryMaxBinaryValue( $dbh, "dp_file_input", "input_id" );

  # Start a transaction.
  $dbh->begin_work unless $DEBUG;

  ###############################################
  # Create the new recipe_instance
  ###############################################

  $sql = "insert into dp_recipe_instance\n";
  $sql .= "  ( recipe_instance_id, recipe_id, state, priority";
  if ($tag) {
    $sql .= ", tag";
  }
  if( defined( $mode ) || defined( $project ) ) {
    $sql .= ", parameters";
  }
  if (defined $queue) {
    $sql .= ", project";
  }
  $sql .= " )\n";
  $sql .= "  values\n";
  $sql .= "  ( $dp_recipe_instance_id, $dp_recipe_id, \" \", $priority";
  if (defined $tag) {
    $sql .= ", \"$tag\"";
  }
  if( defined( $mode ) && defined( $project ) ) {
    $sql .= ", \"-mode='$mode' -drparameters='-recpars recpars-$project.ini $drparams'\"";
  } elsif( defined( $mode ) ) {
    $sql .= ", \"-mode='$mode'" . ($drparams ? " -drparameters='$drparams'" : "") ."\"";
  } elsif( defined( $project ) ) {
    $sql .= ", \"-drparameters='-recpars recpars-$project.ini $drparams'\"";
  }
  if (defined $queue) {
    $sql .= ", \"$queue\"";
  }
  $sql .= " )";
  insertWithRollback( $dbh, $sql);

  ###############################################
  # Fill rows in dp_file_input
  ###############################################

  my $dp_file_input_id;
  my $mem;
  for $mem (@$MEMBERSREF) {
    chomp( $mem );
    $dp_file_input_id = sprintf "0x%016lx", (++$dp_file_input_bigint);
    $sql = <<ENDMEMBER;
insert into dp_file_input
  ( input_id, recipe_instance_id, dp_input, input_role )
  values
  ( $dp_file_input_id, $dp_recipe_instance_id, '$mem', 'infile' )
ENDMEMBER
    insertWithRollback( $dbh, $sql );
  }

  $dbh->commit unless $DEBUG;

  return $dp_recipe_instance_id;

}

sub queryValue {
  my ( $dbh, $sql ) = @_;

  print "VERBOSE: sql=$sql\n" if $VERBOSE;
  my $sth = $dbh->prepare( $sql ) or &cadc_dberror;
  $sth->execute or &cadc_dberror;

  my $value;
  $sth->bind_columns( \$value );
  while ( $sth->fetch ) { }
  $sth->finish;

  return $value
}

# Find the max value of a supplied binary column
# We have to pad trailing zeroes and assume a fixed
# size of 16 characters
# Args are ($dbh, table, $column)
sub queryMaxBinaryValue {
  my ($dbh, $table, $column) = @_;

  my $sql = "select isnull(max($column),0) from $table";
  my $maxval = queryValue( $dbh, $sql );
  return binaryAsBigint( $maxval );
}

# Query a binary value
sub queryBinaryValue {
  my ($dbh, $sql) = @_;
  my $result = queryValue( $dbh, $sql );
  if (defined $result) {
    return bigintstr( binaryAsBigint( $result ) );
  }
  return;
}

# Pad a binary value with trailing zeroes and convert to a BigInt
sub binaryAsBigint {
  my $bin = shift;
  $bin .= "0" x ( 16 - length($bin) );
  return Math::BigInt->new("0x". $bin );
}

# Convert a big int to a string suitable for insertion into
# a sybase BINARY field

sub bigintstr {
  return sprintf( "0x%016lx", $_[0] );
}

sub insertWithRollback {
  my ($dbh, $sql) = @_;

  if ($DEBUG) {
    print "Would be executing: $sql\n";
    return;
  }
  print "VERBOSE: sql=\n$sql\n" if $VERBOSE;

  my $sth = $dbh->prepare( $sql );
  if (!$sth) {
    my $err = $DBI::errstr;
    $dbh->rollback;
    throw JSA::Error::CADCDB( $err );
  }

  if (!$sth->execute) {
    my $err = $DBI::errstr;
    $dbh->rollback;
    throw JSA::Error::CADCDB( $err );
  }
}

=back

=head1 AUTHORS

Brad Cavanagh E<lt>b.cavanagh@jach.hawaii.eduE<gt>
Russell Redman E<lt>Russell.Redman@nrc-cnrc.gc.caE<gt>

=head1 COPYRIGHT

Copyright (C) 2009 Science and Technology Facilities Council. All Rights Reserved.

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
