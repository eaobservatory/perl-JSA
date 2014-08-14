package JSA::Proc;

=head1 NAME

JSA::Proc - Interface to JSA local processing system.

=cut

use strict;

use parent qw/Exporter/;

use Alien::Taco;

our @EXPORT_OK = qw/add_jsa_proc_jobs/;

=head1 SUBROUTINES

=over 4

=item add_jsa_proc_jobs(\%group_local, \%group_cadc)

Adds processing jobs to the JSA local processing system.  Takes references
to two hashes containing the jobs to be run locally, and those running at
CADC.  These should be of the same form as that for the
JSA::Submission::submit_jobs subroutine, except that the CADC jobs should
have their recipe instance added to the hash as "recipe_id".

=cut

{
    sub add_jsa_proc_jobs {
        my $group_local = shift;
        my $group_cadc = shift;
    }
}

=back

=head1 COPYRIGHT

Copyright (C) 2014 Science and Technology Facilities Council.
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
