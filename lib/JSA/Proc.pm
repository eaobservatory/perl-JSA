package JSA::Proc;

=head1 NAME

JSA::Proc - Interface to JSA local processing system.

=cut

use strict;

use parent qw/Exporter/;

use Alien::Taco;
use Data::Dumper; # For "debug" mode.

use JSA::Submission qw/log_message/;

our @EXPORT_OK = qw/add_jsa_proc_jobs/;

=head1 SUBROUTINES

=over 4

=item add_jsa_proc_jobs(\%group_local, \%group_cadc, $mode, $priority, $debug)

Adds processing jobs to the JSA local processing system.  Takes references
to two hashes containing the jobs to be run locally, and those running at
CADC.  These should be of the same form as that for the
JSA::Submission::submit_jobs subroutine, except that the CADC jobs should
have their recipe instance added to the hash as "recipe_id".

=cut

{
    # Taco client for the connection to Python, to be constructed when
    # required.
    my $taco = undef;
    my $jsa_proc_db = undef;

    sub add_jsa_proc_jobs {
        my $group_local = shift;
        my $group_cadc = shift;
        my $mode = shift;
        my $priority = shift;
        my $debug = shift;

        log_message("\nBeginning to add jobs to local jsa_proc system.\n");

        # Create Taco connection to Python unless we already have one.
        unless ($debug or defined $jsa_proc_db) {
            log_message("Opening connection to jsa_proc.\n");
            $taco = new Alien::Taco(lang => 'python');
            $taco->import_module('jsa_proc.config', args => ['get_database']);
            $jsa_proc_db = $taco->call_function('get_database');
        }
        elsif ($debug) {
            log_message("Skipping connection to jsa_proc. [DEBUG MODE]\n");
        }

        foreach ([0, $group_local], [1, $group_cadc]) {
            my ($cadc, $groups) = @$_;
            my $location = $cadc ? 'CADC' : 'JAC';
            log_message("\nProcessing jobs for location: $location\n");

            while (my ($tag, $group) = each %$groups) {
                log_message("\nProcessing job with tag: $tag\n");

                # If this job is supposed to be at CADC, check it has a
                # recipe_id, because if it doesn't, it probably wasn't
                # submitted successfully.
                my $recipe_id = undef;
                if ($cadc) {
                    $recipe_id = $group->{'recipe_id'};
                    unless (defined $recipe_id) {
                        log_message("WARNING: job has no CADC recipe ID.\n",
                                    "         Skipping to next job.\n");
                        next;
                    }
                }

                # Make list of plain file names with the extensions removed.
                my @files = map {s/\.sdf$//; $_} @{$group->{'files'}};

                # Prepare arguments for the jsa_proc database add_job method.
                my %args = (
                    tag             => $tag,
                    location        => $location,
                    mode            => $group->{'mode'} // $mode,
                    parameters      => $group->{'drparams'},
                    input_file_names=> \@files,
                    foreign_id      => $recipe_id,
                    priority        => $priority,
                );

                unless ($debug) {
                    # In non-debug mode, try calling the add_job method.

                    log_message("Adding job to jsa_proc database.\n");
                    eval {
                        my $job_id = $jsa_proc_db->call_method(
                                        'add_job', kwargs => \%args);
                        log_message("Job added with ID: $job_id\n");
                    };
                    if ($@) {
                        log_message("ERROR: failed to add job:\n$@\n");
                    };
                }
                else {
                    # In debug mode, print the arguments we would have given.

                    log_message("Would have added the job: [DEBUG MODE]\n");
                    print Data::Dumper->Dump([\%args], [qw/args/]);
                }
            }
        }

        log_message("\nDone adding jobs to local jsa_proc system.\n")
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
