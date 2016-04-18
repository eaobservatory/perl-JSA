#!perl

use strict;
use warnings;
use Module::Build;

# Copyright (C) 2008-2009 Science and Technology Facilities Council.
# All Rights Reserved.

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place,Suite 330, Boston, MA 02111-1307,
# USA

# Set it up.
my $build = Module::Build->new
  (
   module_name => 'JSA',
   license  => 'gpl',
   dist_abstract => 'JCMT Science Archive helper functions',
   dist_author => [
                   'Tim Jenness <t.jenness@jach.hawaii.edu>',
                   'Anubhav Agarwal <a.agarwal@jach.hawaii.edu>',
                   'Brad Cavanagh <b.cavanagh@jach.hawaii.edu>',
                  ],
   dist_version => '0.03',
   script_files => [ qw/
                         dpRetrieve jsawrapdr dpCapture convert_dr_files jsasubmit manual_submit
                         jsaerrors jsahpxobs jsahpxharpnit
                       / ],
   meta_merge => {
               resources =>  {
                              repository => "git://github.com/eaobservatory/perl-JSA.git",
                             },
                },
   requires => {
                'perl'          => '5.8.0',
                'Proc::SafeExec' => 0,
                'Astro::FITS::Header' => 3,
                'Astro::FITS::HdrTrans' => 1.0,
                'File::Temp' => 0.20,
                'File::Remove' => 0,
                'Config::IniFiles' => 0,
                'DateTime' => 0,
                'NDF' => 0,
                'Starlink::Versions' => 0,
                'Statistics::Descriptive' => 0,
                'Time::Piece' => 0,
                'URI' => 0,
                'Pod::Usage' => 0,
                'Astro::Coords' => 0,
                'Image::ExifTool' => 0,
                'JAC::Setup' => 0.01,
                'Log::Log4perl' => 0,
                'File::HomeDir' => 0,
               },
   recommends => {
                  'DBD::Sybase' => 0,
                 },
   build_requires => {
                      'Test::More' => 0,
                      'Test::Command' => 0,
                      'JAC::Setup' => 0.02,
                     },
   configure_requires => {
                          "Module::Build" => 0.30,
                         },
  );

$build->create_build_script;
