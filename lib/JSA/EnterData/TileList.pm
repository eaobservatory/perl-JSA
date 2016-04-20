package JSA::EnterData::TileList;

use strict; use warnings;

our $VERSION = '0.02';

=pod

=head1 NAME

JSA::EnterData::TileList - Methods related to tile numbers.

=head1 SYNOPSIS

    use JSA::EnterData::TileList;
    use JSA::EnterData::StarCommand;

    $tilenum = new JSA::EnterData::TileList();
    $starcom = new JSA::EnterData::StarCommand();

    $starcom->try_command('command' => [$tilenum->get_file_command('/file/list')])
        or die "Could not run the tile numbers finding command.";

    print Dumper($starcom->get_value('tilenum'));

=head1 DESCRIPTION

This object oriented module has methods related to Starlink command execution.

=head2 METHODS

=over 2

=cut

use Carp ();
use Log::Log4perl ;

use JSA::Error qw/:try/;

our $Tile_List = '/star/bin/smurf/jsatilelist';

=item B<new> (constructor)

Returns a L<JSA::EnterData::TileList> object.

    $tilenum = new JSA::EnterData::TileList();

Takes an optional argument of truth value if to use F</stardev> instead of
F</star>.

    $tilenum_in_hilo = new JSA::EnterData::TileList('dev');

=cut

sub new {
    my ($class, $dev) = @_;

    # FIXME: this alters the package variable rather than this instance.
    $Tile_List =~ s/^(\/star)\//${1}dev\// if $dev;

    my $obj = '';
    return $obj = bless \$obj, $class;
}

=pod

=item B<get_file_command>

Given a file list path, returns a list of F<jsatilelist> command and arguments to
be run.

    @command = $tilenum->get_file_command('/file/list');

=cut

sub get_file_command {
    my ($class, $list) = @_;

    throw JSA::Error::BadArgs 'No readable file list given.'
        unless defined $list && -r $list;

    return ($Tile_List, "in=^$list");
}


=pod

=item B<get_radec_command>

Given a instrument name and 4 corners as RA & Dec array references, returns a
list of F<jsatilelist> command and arguments to be run.

    @command = $tilenum->get_radec_command('RxWD',
                                           [$obsratl, $obsratr,
                                            $obsrabr, $obsrabl
                                           ],
                                           [$obsdectl, $obsdectr,
                                            $obsdecbr, $obsdecbl
                                           ]);

Known instruments are ...

    HARP
    RxA
    RxWB
    RxWD
    SCUBA-2(450)
    SCUBA-2(850)

=cut

{
    my $inst_ok;

    sub get_radec_command {
        my ($class, $inst, $ra, $dec) = @_;

        my @ra  = @{$ra};
        my @dec = @{$dec};

        unless (defined $inst_ok) {
            $inst_ok = qr/^(?:$_)$/i
                for join '|', map quotemeta($_), qw/
                    DAS
                    ACSIS
                    HARP RxA RxWB RxWD
                    SCUBA-2(450) SCUBA-2(850)
                /;
        }

        throw JSA::Error::BadArgs "Unknown instrument name, $inst, given."
            unless $inst =~ $inst_ok;

        $inst = 'ACSIS' if $inst =~ /\b (HARP\b | Rx) /xi;

        return (
            $Tile_List,
            "in=!",
            "instrument=$inst",
            'vertex_ra=['  . join(',', @ra)  . ']',
            'vertex_dec=[' . join(',', @dec) . ']');
    }
}


1;

=pod

=back

=head1 COPYRIGHT, LICENSE

Copyright (C) 2013-2014 Science and Technology Facilities Council.
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
