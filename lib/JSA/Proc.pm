package JSA::Proc;

=head1 NAME

JSA::Proc - Interface to JSA local processing system.

=cut

use strict;

use parent qw/Exporter/;

use Alien::Taco;
use Data::Dumper; # For "debug" mode.

use JSA::Submission qw/log_message/;

our @EXPORT_OK = qw/add_jsa_proc_jobs create_obsinfo_hash/;

=head1 SUBROUTINES

=over 4

=item add_jsa_proc_jobs(\%groups, $mode, $priority, $add_info_only, $debug, %options)

Adds processing jobs to the JSA local processing system.
C<\%groups> is a reference to a hash holding groups and information about them.
Keys are the group association id string.
Values are hash references containing keys:

=over 4

=item files

Array of files for this group.

=item drparams

DR parameters.

=item mode

Mode to use for processing.

=item recpars

Recipe parameters file name.

=item obsinfolist

List of observation information hashes,
e.g. created with C<create_obsinfo_hash>.

=item task

Processing task name.

=back

If the C<$add_info_only> option is set then instead of adding jobs, observation
info is added to jobs which currently lack it.

Options include:

=over 4

=item allow_update

Call JSA Proc "add_upd_del_job" instead of using plain "add_job" method.

=item jsa_proc_logging

Enables logging from the JSA Proc system.  Should be the name of a Python
logging level (e.g. INFO or DEBUG).

B<Note:> only applies in first call which creates connection to JSA Proc.

B<Note:> probably only useful in conjunction with "allow_update".

=item jsa_proc_dry_run

Enables dry-run mode in JSA Proc system.

B<Note:> only applies when used in conjunction "allow_update"
but not with C<$add_info_only>.

=back

=cut

{
    # Taco client for the connection to Python, to be constructed when
    # required.
    my $taco = undef;
    my $jsa_proc_db = undef;

    sub add_jsa_proc_jobs {
        my $groups = shift;
        my $mode = shift;
        my $priority = shift;
        my $add_info_only = shift;
        my $debug = shift;
        my %options = @_;

        log_message("\nBeginning to add jobs to local jsa_proc system.\n");

        # Create Taco connection to Python unless we already have one.
        # Submission mode debugging does not require a connection, but
        # info adding debugging does.
        unless (($debug and not $add_info_only) or defined $jsa_proc_db) {
            log_message("Opening connection to jsa_proc.\n");
            $taco = new Alien::Taco(lang => 'python');
            $taco->import_module('jsa_proc.config', args => ['get_database']);
            $taco->import_module('jsa_proc.submit.update',
                                 args => ['add_upd_del_job']);
            $jsa_proc_db = $taco->call_function('get_database');

            if ($options{'jsa_proc_logging'}) {
                $taco->import_module('logging');
                $taco->call_function(
                    'logging.basicConfig', kwargs => {level =>
                        $taco->get_value(
                            'logging.' . uc($options{'jsa_proc_logging'}))});
            }
        }
        elsif ($debug) {
            log_message("Skipping connection to jsa_proc. [DEBUG MODE]\n");
        }

        my $location = 'JAC';
        log_message("\nProcessing jobs for location: $location\n");

        while (my ($tag, $group) = each %$groups) {
            log_message("\nProcessing job with tag: $tag\n");

            # Make list of plain file names with the extensions removed.
            my @files = map {s/\.sdf$//; $_} @{$group->{'files'}};


            unless ($add_info_only) {
                # Job submission mode.

                # Prepare arguments for the jsa_proc database add_job method.
                my @param = ();
                push @param, '--recpars', $group->{'recpars'}
                    if exists $group->{'recpars'};
                push @param, $group->{'drparams'}
                    if exists $group->{'drparams'};

                my %args = (
                    tag             => $tag,
                    location        => $location,
                    mode            => $group->{'mode'} // $mode,
                    parameters      => join(' ', @param),
                    input_file_names=> \@files,
                    priority        => $priority,
                    obsinfolist     => $group->{'obsinfolist'},
                    task            => $group->{'task'},
                );

                unless ($debug) {
                    # In non-debug mode, try calling the add_job method.

                    log_message("Adding job to jsa_proc database.\n");
                    eval {
                        my $job_id;
                        unless ($options{'allow_update'}) {
                            $job_id = $jsa_proc_db->call_method(
                                'add_job', kwargs => \%args);
                        }
                        else {
                            $args{'db'} = $jsa_proc_db;
                            $args{'dry_run'} = $options{'jsa_proc_dry_run'};
                            $job_id = $taco->call_function(
                                'add_upd_del_job', kwargs => \%args);
                        }
                        log_message("Job added/updated with ID: $job_id\n");
                    };
                    if ($@) {
                        log_message("ERROR: failed to add/update job:\n$@\n");
                    }
                }
                else {
                    # In debug mode, print the arguments we would have given.

                    log_message("Would have added the job: [DEBUG MODE]\n");
                    log_message(Data::Dumper->Dump([\%args], [qw/args/]));
                }
            }
            else {
                # Info adding mode.

                # Find the jsa_proc job ID based on the tag.
                my $job_id = eval {
                    my @job = $jsa_proc_db->call_method(
                        'get_job', kwargs => {tag => $tag});
                    $job[0];
                };
                unless (defined $job_id) {
                    log_message("WARNING: could not find job by tag '$tag'\n");
                    next;
                }
                log_message("Found job ID: '$job_id'\n");

                # See if the job already has obs info.
                eval {
                    my @existing_info = $jsa_proc_db->call_method(
                        'get_obs_info', args => [$job_id]);

                    if (@existing_info) {
                        log_message("Job already has info\n");
                        next;
                    }

                    log_message("Job needs info adding\n");
                };
                if ($@) {
                    log_message("Failed to check if job already has info\n");
                    next;
                }

                # Job did not have info: prepare to add it.
                my %args = (
                    job_id      => $job_id,
                    obsinfolist => $group->{'obsinfolist'},
                );

                unless ($debug) {
                    # Non-debug mode: attempt to update the job info.

                    log_message("Updating obs info in jsa_proc database.\n");
                    eval {
                        $jsa_proc_db->call_method(
                                'set_obs_info', kwargs => \%args);
                    };
                    if ($@) {
                        log_message("ERROR: failed to update obs info.\n");
                    }
                }
                else {
                    # Debug mode: print the arguments we would have submitted.

                    log_message("Would have updated the job: [DEBUG MODE]\n");
                    log_message(Data::Dumper->Dump([\%args], [qw/args/]));
                }
            }
        }

        log_message("\nDone adding jobs to local jsa_proc system.\n")
    }
}

=item create_obsinfo_hash($obs, \%subsyshdr)

Creates an obsinfo hash and returns a reference to it.

Does not include association.

=cut

sub create_obsinfo_hash {
    my $obs = shift;
    my $subsyshdr = shift;

    my $subsys;

    if ($subsyshdr->{'BACKEND'} eq 'SCUBA-2') {
        $subsys = $subsyshdr->{'FILTER'};
    }
    elsif ($subsyshdr->{'BACKEND'} eq 'ACSIS') {
        $subsys = $subsyshdr->{'SUBSYSNR'};
    }
    else {
        die 'Do not know how to derive subsystem for backend: ' .
            $subsyshdr->{'BACKEND'};
    }

    return {
            obsid =>        $subsyshdr->{'OBSID'},
            obsidss =>      $subsyshdr->{'OBSID_SUBSYSNR'},
            date_obs =>     $obs->startobs()->strftime('%Y-%m-%d %H:%M:%S'),
            utdate =>       $subsyshdr->{'UTDATE'},
            obsnum =>       0 + $subsyshdr->{'OBSNUM'},
            instrument =>   $subsyshdr->{'INSTRUME'},
            backend =>      $subsyshdr->{'BACKEND'},
            subsys =>       $subsys,
            project =>      $subsyshdr->{'PROJECT'},
            survey =>       $subsyshdr->{'SURVEY'},
            scanmode =>     (($subsyshdr->{'SAM_MODE'} eq 'scan')
                              ? $subsyshdr->{'SCAN_PAT'}
                              : $subsyshdr->{'SAM_MODE'}),
            sourcename =>   $subsyshdr->{'OBJECT'},
            obstype =>      $subsyshdr->{'OBS_TYPE'},
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
