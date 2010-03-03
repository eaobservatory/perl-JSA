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

# Define connection information.
my $READDATABASE = "jcmtmd";
my $WRITEDATABASE = "data_proc";
my $DBSERVER = "CADC_ASE";
my $AD = "JCMT";
my $DBUSER = OMP::Config->getData( 'cadc_dp.user' );
my $DBPSWD = OMP::Config->getData( 'cadc_dp.password' );

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

 - mode: Grouping mode ("night", "project", "public")

This function returns the recipe instance ID on success, or undef for failure.

=cut

sub create_recipe_instance {
  my $dbh = shift;
  my $MEMBERSREF = shift;

  my $options = shift;

  my $mode = $options->{'mode'};
  my $project = defined( $options->{'project'} ) ? uc( $options->{'project'} ) : undef;

  my $sql;

  print "VERBOSE: create a row in dp_recipe_instance\n" if $VERBOSE;

  ###############################################
  # First, find the recipe_id for jsawrapdr
  ###############################################

  $sql = <<ENDRECIPEID;
select recipe_id
   from $WRITEDATABASE..dp_recipe
   where script_name="jsawrapdr"
ENDRECIPEID
  print "VERBOSE: sql=\n$sql\n" if $VERBOSE;

  my $dp_recipe_id = 0;
  my $sth = $dbh->prepare( $sql ) or &dbError;
  $sth->execute or &dbError;
  $sth->bind_columns( \$dp_recipe_id );
  while ( $sth->fetch ) {}
  $sth->finish;

  throw JSA::Error::CADCDB( "Cannot retrieve good recipe_id from dp_recipe" )
    unless $dp_recipe_id;

  ###############################################
  # Use the maximum current value of recipe_instance_id in
  # dp_recipe_instance to generate a "new" recipe_instance_id
  ###############################################

  $sql = <<ENDNEWID;
select recipe_instance_id
   from dp_recipe_instance
   order by recipe_instance_id asc
ENDNEWID
  print "VERBOSE: sql=\n$sql\n" if $VERBOSE;

  my $dp_recipe_instance_count = queryValue( $dbh, $sql );
  my $dp_recipe_instance_bigint = Math::BigInt->new("0x" . $dp_recipe_instance_count)+1;
  my $dp_recipe_instance_id = sprintf "0x%016lx", $dp_recipe_instance_bigint;

  ###############################################
  # Use the maximum cuurent value of input_id in
  # dp_file_input to generate a "new" input_id
  ###############################################

  $sql = <<ENDNEWFILEID;
select isnull(max(input_id),0)
   from dp_file_input
ENDNEWFILEID

$sql = "select input_id from dp_file_input order by input_id";

  print "VERBOSE: sql=\n$sql\n" if $VERBOSE;

  my $dp_file_input_count = queryValue( $dbh, $sql );
  my $dp_file_input_bigint = Math::BigInt->new("0x".$dp_file_input_count);
  my $dp_file_input_id;

  # Start a transaction.
  $dbh->begin_work;

  ###############################################
  # Create the new recipe_instance
  ###############################################

  $sql = "insert into dp_recipe_instance\n";
  $sql .= "  ( recipe_instance_id, recipe_id, state";
  if( defined( $mode ) || defined( $project ) ) {
    $sql .= ", parameters";
  }
  $sql .= " )\n";
  $sql .= "  values\n";
  $sql .= "  ( $dp_recipe_instance_id, 0x$dp_recipe_id, \" \"";
  if( defined( $mode ) && defined( $project ) ) {
    $sql .= ", \"-mode='$mode' -drparameters='-recpars recpars-$project.ini'\"";
  } elsif( defined( $mode ) ) {
    $sql .= ", \"-mode='$mode'\"";
  } elsif( defined( $project ) ) {
    $sql .= ", \"-drparameters='-recpars recpars-$project.ini'\"";
  }
  $sql .= " )";
  print "VERBOSE: sql=\n$sql\n" if $VERBOSE;
  insertWithRollback( $dbh, $sql);

  ###############################################
  # Fill rows in dp_file_input
  ###############################################

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
print "VERBOSE: sql=\n$sql\n" if $VERBOSE;
    insertWithRollback( $dbh, $sql );
  }

  $dbh->commit;

  return $dp_recipe_instance_id;

}

sub queryValue {
  my ( $dbh, $sql ) = @_;

  my $sth = $dbh->prepare( $sql ) or &cadc_dberror;
  $sth->execute or &cadc_dberror;

  my $value;
  $sth->bind_columns( \$value );
  while ( $sth->fetch ) { }
  $sth->finish;

  return $value
}

sub insertWithRollback {
  my ($dbh, $sql) = @_;

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
