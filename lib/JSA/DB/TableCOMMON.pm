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
        msstart
        req_mintau
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
        msend
        req_maxtau
        seedaten seeingen seqend
        tau225en taudaten
        wnddiren wndspden wvmdaten wvmtauen
      /;
}

sub range_columns {
    my %range;
    @range{range_start_columns()} = range_end_columns();
    return %range;
}

1;
