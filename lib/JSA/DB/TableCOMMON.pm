package  JSA::DB::TableCOMMON;

use strict;
use warnings;

sub range_start_columns {
    return qw/
        amstart atstart azstart
        bklegtst bpstart
        date_obs
        elstart
        frlegtst
        hststart humstart
        lststart
        seedatst seeingst seqstart
        tau225st taudatst
        wnddirst wndspdst wvmdatst wvmtaust
      /;
}

sub range_end_columns {
    return qw/
        amend atend azend
        bklegten bpend
        date_end
        elend
        frlegten
        hstend humend
        lstend
        seedaten seeingen seqend
        tau225en taudaten
        wnddiren wndspden wvmdaten wvmtauen
      /;
}

1;
