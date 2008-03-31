#!perl

use strict;
use warnings;
use ExtUtils::MakeMaker;
use File::Spec;

# Copyright (C) 2008 Science and Technology Facilities Council.
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

WriteMakefile( 
               'NAME'           => 'JSA',
               'VERSION'        => '0.01',
               'PREREQ_PM'      => { 
                                    'Test::More' => 0,
                                    'Test::Command' => 0,
                                    'Proc::SafeExec' => 0,
                                    'Astro::FITS::Header' => 3,
                                    'Astro::FITS::HdrTrans' => 1.0,
                                   },
              EXE_FILES => [qw/
                                dpRetrieve jsawrapdr dpCapture
                              /],
               'dist'           => { COMPRESS => "gzip -9f"},
               ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
               ( ABSTRACT       => 'JCMT Science Archive helper functions',
                 AUTHOR         => 'Tim Jenness E<lt>t.jenness@jach.hawaii.eduE<gt>') : ()),
             );


