package JSA::CADC_Copy;

=head1 NAME

JSA::CADC_Copy - Check JCMT data in the archive at CADC

=head1 SYNOPSIS

Determine which files are present at CADC:

    my $files = JSA::CADC_Copy::at_cadc($date);

=head1 DESCRIPTION

This module provides a subroutine for checking whether
data files are at CADC.

=cut

use warnings;
use strict;

use List::MoreUtils qw/any/;
use LWP::UserAgent;
use URI::Escape;
use Astro::VO::VOTable::Document;

=head2 FUNCTIONS

=over 2

=item B<at_cadc>

Return a list of files uploaded to CADC for a specific UT date.

  my $at_cadc = at_cadc( $ut );

The UT date must be in YYYYMMDD format. This function returns a hash
reference, with keys being the files at CADC. If the UT date is in an
incorrect format, this method returns undef. If no files are returned,
this method returns an empty hash reference.

Return a list of files uploaded to CADC for a specific UT date.

Accepts optional time (in seconds) with key of C<wait> to wait between
requests to CADC server; and, an optional array reference of
instrument prefixes as with key of C<prefix>.

The instrument prefixes are ...

  a,
  s4a,
  s4b,
  s4c,
  s4d,
  s8a,
  s8b,
  s8c,
  s8d


If no list is given, all of the above prefixes are used.

  my $at_cadc = at_cadc( $ut,
                          'wait'   => 2,
                          'prefix' => [ qw[ a s4a s8d ] ]
                        );

  $at_cadc = at_cadc( $ut, 'prefix' => [ qw[ s4a s8d ] ] );


This function returns a hash reference, with keys being the files at CADC. If
the UT date is in an incorrect format, this method returns undef. If no files
are returned, this method returns an empty hash reference.

Accepts also an optional array reference of instrument prefixes with
key of "prefix", out
of...

  a,
  s4a,
  s4b,
  s4c,
  s4d,
  s8a,
  s8b,
  s8c,
  s8d

... and, wait time in seconds to wait between requests to CADC
server with key of "wait".

B<Note:> a valid proxy certificate must be present at C<~/.ssl/cadcproxy.pem>.

=cut

sub at_cadc {
    my ($ut, %opt) = @_;

    return if defined $ut && $ut !~ /^\d{8}$/;

    my @inst = qw/a s4a s4b s4c s4d s8a s8b s8c s8d/;

    my @prefix =
      ($opt{'prefix'} && ref $opt{'prefix'} ? @{ $opt{'prefix'} } : ());

    # Use instrument prefix and the date.
    if ($ut) {
        @prefix = @inst unless scalar @prefix;

        return _check_cadc($opt{'wait'}, map { "${_}${ut}" } @prefix);
    }

    # Assume to be file names.
    my @file;
    for my $f (@prefix) {
        push @file, $f
            if any { $f =~ /^$_\d{8}/ } @inst;
    }

    return _check_cadc($opt{'wait'}, @file);
}

sub _check_cadc {
    my ($wait, @prefix) = @_;

    return unless scalar @prefix;

    # Time to wait for a random, reasonable amount.
    $wait //= 20;

    my $cadc_url = 'https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/ad/sync';

    # To avoid hammering the server when run multiple times in a row.
    my $sleepy_time = scalar(@prefix) - 1;

    # Go through each instrument prefix and push the list of files onto
    # our array.
    my @uploaded;
    foreach my $prefix (@prefix) {
        my $query = sprintf "SELECT fileName FROM archive_files WHERE archiveName = 'JCMT' AND fileName LIKE '%s%%'", $prefix;
        my $ua = new LWP::UserAgent(timeout => 60);
        $ua->ssl_opts(
            SSL_use_cert => 1,
            SSL_cert_file => $ENV{'HOME'} . '/.ssl/cadcproxy.pem');
        my $res = $ua->get($cadc_url . '?REQUEST=doQuery&LANG=ADQL&QUERY=' . uri_escape($query));
        next unless $res->is_success;

        # VOTable-reading code based on that from Astro::Catalog::IO::VOTable.
        my $doc = Astro::VO::VOTable::Document->new_from_string($res->content());
        my $votable = ($doc->get_VOTABLE())[0];
        my $resource = ($votable->get_RESOURCE())[0];
        my $table = ($resource->get_TABLE())[0];
        my $data = ($table->get_DATA())[0];
        my $tabledata = ($data->get_TABLEDATA())[0];
        my $nrows = $tabledata->get_num_rows();
        next unless defined $nrows;

        foreach my $j (0 ... $nrows - 1) {
            my @row = $tabledata->get_row($j);
            push @uploaded, $row[0];
        }

        $sleepy_time-- > 0 and sleep $wait;
    }

    my %at_cadc = map {chomp( $_ ); $_ => undef} @uploaded;

    return \%at_cadc;
}

1;

=back

=head1 AUTHORS

Anubhav E<lt>a.agarwal@jach.hawaii.eduE<gt>,
Brad Cavanagh E<lt>b.cavanagh@jach.hawaii.eduE<gt>.

=head1 COPYRIGHT

Copyright (C) 2008, 2009 Science and Technology Facilities Council.
All Rights Reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or (at
your option) any later version.

This program is distributed in the hope that it will be useful,but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place,Suite 330, Boston, MA  02111-1307,
USA

=cut
