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
our $DEBUG = 0;  # Do not write to the database

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
 - recpars: A recipe parameters override. Assumes that a recipe parameter
            file name is given as used by ORAC-DR.
 - priority: Relative priority of job as an integer. Default
             priority will be -1. Range is between -500 and 0.
 - queue: Name of CADC processing queue (can be undef)
 - drparams: Additional options for jsawrapdr pipeline. Would usually be
          a recipe name.
 - dprecipe: Constant indicating which CADC processing recipe to use to reduce the data.
          Default will be CADC_DPREC_8G.

This function returns the recipe instance ID on success, or undef for failure. Returns
"-0x???" if the recipe could not be inserted because an entry already exists that is in
the wrong state.

=cut

sub create_recipe_instance {
  my $dbh = shift;
  my $MEMBERSREF = shift;

  my $options = shift;

  my $mode = $options->{'mode'};
  my $recpars = ( defined( $options->{recpars} ) ? $options->{recpars} : undef );
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

  # We first need to see if the supplied tag already exists in the recipe
  # instance table.
  $sql = "SELECT * FROM dp_recipe_instance WHERE tag = '$tag' ";
  my %dp_recipe_instance_info = querySingleRow( $dbh, $sql, [ "recipe_instance_id",
                                                              "recipe_id" ]);

  my $updating = 0;
  my $dp_recipe_instance_id;
  # if we found it we can reuse the instance
  if ( keys %dp_recipe_instance_info ) {

    # Convert binary to string form
    for my $k (qw/ recipe_instance_id recipe_id /) {
      $dp_recipe_instance_info{$k} = bigintstr( $dp_recipe_instance_info{$k} );
    }

    $dp_recipe_instance_id = $dp_recipe_instance_info{recipe_instance_id};

    # We have found a pre-existing instance with this tag but we can only
    # proceed if the instance is in the "E" or "Y" state.

    if ( $dp_recipe_instance_info{state} !~ /^[EY]$/) {
      if ($VERBOSE) {
        print "Skipping update for recipe_id $dp_recipe_instance_id as it is in state '$dp_recipe_instance_info{state}'\n";
      }
      return "-" . $dp_recipe_instance_id;
    }

    $updating = 1;

  } else {

    ###############################################
    # Use the maximum current value of recipe_instance_id in
    # dp_recipe_instance to generate a "new" recipe_instance_id
    ###############################################

    my $dp_recipe_instance_bigint = queryMaxBinaryValue( $dbh, "dp_recipe_instance",
                                                         "recipe_instance_id") +1;
    $dp_recipe_instance_id = bigintstr($dp_recipe_instance_bigint);
  }

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

  # Form a hash with all the relevant content
  my %dp_recipe_instance = (
                            recipe_instance_id => $dp_recipe_instance_id,
                            recipe_id => $dp_recipe_id,
                            state => " ",
                            priority => $priority,
                           );

  $dp_recipe_instance{tag} = $tag if defined $tag;
  $dp_recipe_instance{project} = $queue if defined $queue;

  # DR parameters
  if ( defined $mode || defined $recpars || defined $drparams) {
    my @paramlist;
    push(@paramlist, "-mode='$mode'") if defined $mode;

    my @droptions;
    push(@droptions, "-recpars $recpars") if defined $recpars;
    push(@droptions, $drparams) if $drparams;
    push(@paramlist, "-drparameters='". join(" ", @droptions) ."'")
      if @droptions;

    $dp_recipe_instance{parameters} = join(" ", @paramlist);
  }

  if ($updating) {
    # if we are updating then we need to find out what needs to be
    # changed and just update that. At the very least we will
    # be changing state back to " ".
    my %dp_recipe_update;
    for my $k ( keys %dp_recipe_instance ) {
      if ( $dp_recipe_instance_info{$k} ne $dp_recipe_instance{$k}) {
        $dp_recipe_update{$k} = $dp_recipe_instance{$k};

        # tag and recipe_instance_id MUST match
        if ($k eq 'tag' || $k eq 'recipe_instance_id') {
          print "Comparing $k => $dp_recipe_instance_info{$k} (DB) vs $dp_recipe_instance{$k} (NEW)\n"
            if $DEBUG;
          JSA::Error::CADCDB->throw( "Can not be updating with differing tags or recipe_instance_id" );
        }
      }
    }

    updateWithRollback( $dbh, "dp_recipe_instance",
                        { recipe_instance_id => $dp_recipe_instance_id },
                        %dp_recipe_update );

  } else {
    insertWithRollback( $dbh, "dp_recipe_instance", \%dp_recipe_instance);
  }

  ###############################################
  # Fill rows in dp_file_input
  ###############################################

  # We either have no files in the table, files in the table
  # or files to remove from the table. So if we are updating
  # we first find what files are in the table already and then
  # remove those from the list of files to be added. We also
  # remove any entries from the database that are no longer
  # relevant
  my @to_add;
  if ($updating) {
    my @results = runQuery( $dbh,
                            "SELECT input_id, dp_input FROM dp_file_input WHERE recipe_instance_id = $dp_recipe_instance_id",
                            [ qw/ input_id /]
                          );

    # Flatten into a simple hash indexed by dp_input
    my %files_in_db;
    for my $r (@results) {
      $files_in_db{$r->{dp_input}} = bigintstr($r->{input_id});
    }

    # Convert the requested files into a hash for ease of
    # use (since order doesn't matter anyhow)
    my %files_to_add = map { $_ => undef } @$MEMBERSREF;

    # Remove any files that are in both hashes
    for my $f (@$MEMBERSREF) {
      if (exists $files_in_db{$f}) {
        delete $files_in_db{$f};
        delete $files_to_add{$f};
      }
    }

    # Anything left in files_in_db need to be deleted
    if (keys %files_in_db) {
      $sql = "DELETE FROM dp_file_input WHERE input_id = ?";
      executeWithRollback( $dbh, $sql, values %files_in_db );
    }

    # Anything left in files_to_add needs to be added
    @to_add = keys %files_to_add;

  } else {
    @to_add = @$MEMBERSREF;
  }

  # Just add anything still to add. Do it with a single statement handle
  my @inserts;
  for my $mem (@to_add) {
    chomp( $mem );
    my $dp_file_input_id = sprintf "0x%016lx", (++$dp_file_input_bigint);
    my %dp_file_input = ( input_id => $dp_file_input_id,
                          recipe_instance_id => $dp_recipe_instance_id,
                          dp_input => $mem,
                          input_role => "infile" );
    push(@inserts, \%dp_file_input);
  }
  insertWithRollback( $dbh, "dp_file_input", @inserts );

  $dbh->commit unless $DEBUG;

  return $dp_recipe_instance_id;

}

# Retrieve a single result from a query

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

# Executre a query and return the results as an array
# of hashes
#   @results = runQuery( $dbh, $sql, $bin );
# Optional 3rd argument lists binary columns that need to
# be converted to Math::Bigints

sub runQuery {
  my ($dbh, $sql, $bin) = @_;

  print "VERBOSE sql=$sql\n" if $VERBOSE;
  my $sth = $dbh->prepare( $sql ) or &cadc_dberror;
  $sth->execute or &cadc_dberror;

  my @rows;
  while ( my $data = $sth->fetchrow_hashref) {
    # Process any binary results
    if (defined $bin) {
      for my $b (@$bin) {
        if (exists $data->{$b}) {
          $data->{$b} = binaryAsBigint( $data->{$b} );
        } else {
          print "WARNING: Expected column $b from query results but it was missing\n";
        }
      }
    }
    # Store it
    push(@rows, $data);
  }
  $sth->finish;

  return @rows
}

# Query the database and return the last matching row as
# a hash. We trigger an error if we get more than one row.
# Returns empty list if there are no results.
# Optional 3rd argument lists binary columns that need to
# be converted to Math::BigInts.

sub querySingleRow {
  my @results = runQuery( @_ );

  if (@results == 0) {
    return ();
  } elsif (@results > 1) {
    JSA::Error::CADCDB->throw( "Got ".@results ." results from SQL '$_[1]' when expected only one" );
  }
  return %{$results[0]};
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

# Takes some SQL and an array of items that will be used
# for the placeholder. "execute" assumes one placeholder only
# and execute will be called once for each item.

sub executeWithRollback {
  my $dbh = shift;
  my $sql = shift;
  my @items = @_;

  if ($DEBUG || $VERBOSE) {
    print "".($DEBUG ? "Would be " : "") .
      "Executing SQL = $sql for each item:\n";
    print join("\n",@items)."\n";
    return if $DEBUG;
  }

  my $sth = $dbh->prepare( $sql );
  if (!$sth) {
    my $err = $DBI::errstr;
    $dbh->rollback;
    JSA::Error::CADCDB( $err );
  }

  for my $i (@items) {
    if (!$sth->execute( $i )) {
      my $err = $DBI::errstr;
      $dbh->rollback;
      JSA::Error::CADCDB( $err );
    }
  }
  $sth->finish;

}


# Insert with rollback using placeholders
# Given a database handle, a table name and a hash
# of row information. Multiple hash references can be
# supplied for efficient reuse of statement handle.

sub insertWithRollback {
  my $dbh = shift;
  my $table = shift;
  my @rows = @_;

  # nothing to insert?
  return unless @rows;

  # Assume that each entry has the same keys

  my @columns = sort keys %{$rows[0]};
  my $sql = "INSERT INTO $table (". join(", ", @columns).
    ") VALUES (". join(", ", map { "?" } (0..$#columns)) . ")";

  if ($DEBUG || $VERBOSE) {
    print "". ($DEBUG ? "Would be " : "").
      "Executing: '$sql'\n for ".@rows." rows with arguments:\n";
    for my $row (@rows) {
      print join(",", map { $row->{$_} } @columns) ."\n";
    }
    return if $DEBUG;
  }

  my $sth = $dbh->prepare( $sql );
  if (!$sth) {
    my $err = $DBI::errstr;
    $dbh->rollback;
    throw JSA::Error::CADCDB( $err );
  }

  for my $row (@rows) {
    my @values = map { $row->{$_} } @columns;
    if (!$sth->execute(@values)) {
      my $err = $DBI::errstr;
      $dbh->rollback;
      throw JSA::Error::CADCDB( $err );
    }
  }
  $sth->finish;
}

# Update entries in a table with rollback

sub updateWithRollback {
  my $dbh = shift;
  my $table = shift;
  my $whereref = shift;
  my %updates = @_;

  JSA::Error::CADCDB->throw( "Must supply a where clause for an update!")
      unless (defined $whereref && scalar keys %$whereref);

  my @updcolumns = sort keys %updates;
  my @wherecols = sort keys %$whereref;
  my $sql = "UPDATE $table SET ".
    join(", ", map { " $_ = ? " } @updcolumns) .
      " WHERE " .
        join( " AND ", map { " $_ = ? " } @wherecols);

  if ($DEBUG || $VERBOSE) {
    print "". ($DEBUG ? "Would be " : "").
      "Executing: '$sql'\n with arguments:\n";
    print join(",", map { $updates{$_} } @updcolumns) ."\n";
    print " and WHERE clause: ". join(" AND ", map { $whereref->{$_} } @wherecols)."\n";
    return if $DEBUG;
  }

  my $sth = $dbh->prepare( $sql );
  if (!$sth) {
    my $err = $DBI::errstr;
    $dbh->rollback;
    throw JSA::Error::CADCDB( $err );
  }

  my @values = map { $updates{$_} } @updcolumns;
  push(@values, map { $whereref->{$_} } @wherecols);
  if (!$sth->execute(@values)) {
    my $err = $DBI::errstr;
    $dbh->rollback;
    throw JSA::Error::CADCDB( $err );
  }
  $sth->finish;

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
