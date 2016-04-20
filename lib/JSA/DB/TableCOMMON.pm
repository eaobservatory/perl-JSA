package  JSA::DB::TableCOMMON;

use strict;
use warnings;

my $_name = 'COMMON';

sub table {
    return $_name;
}

sub column_names {
    return qw/
        agentid align_dx align_dy alt_obs amend amstart atend atstart azend
        azstart
        backend bklegten bklegtst bpend bpstart
        chop_crd chop_frq chop_pa chop_thr
        date_end date_obs daz del drgroup dut1
        elend elstart etal
        focaxis focstep focus_dz frlegten frlegtst
        hstend hststart humend humstart
        inbeam inbeam_orig instap instap_x instap_y instrume int_time
        jigl_cnt jigl_crd jigl_nam jigl_pa jig_scal jos_min jos_mult
        last_modified lat_obs locl_crd long_obs lstend lststart
        map_hght map_pa map_wdth map_x map_y msbid msbtid
        moving_target
        nfocstep num_cyc
        object obsdec obsdecbl obsdecbr obsdectl obsdectr obsgeo_x obsgeo_y
        obsgeo_z obsid obsnum obsra obsrabl obsrabr obsratl obsratr obs_type
        ocscfg origin
        pol_conn pol_mode project
        recipe release_date req_mintau req_maxtau rmtagent rotafreq
        sam_mode scan_crd scan_dy scan_pa scan_pat scan_vel seedaten seedatst
        seeingen seeingst standard startidx status steptime survey sw_mode
        tau225en tau225st taudaten taudatst tausrc telescop
        uaz uel utdate
        wnddiren wnddirst wndspden wndspdst wvmdaten wvmdatst wvmtauen wvmtaust
    /;
}

sub date_columns {
    return qw/
        date_obs date_end
        hststart hstend
        release_date
        taudatst taudaten
        wvmdatst wvmdaten
        seedatst seedaten
    /;
}

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

=item B<unique_keys>

Returns list of columns to uniquely identify a row.

    @keys = JSA::DB::TableCOMMON->unique_keys();

=cut

sub unique_keys {
   return qw/obsid/;
}


1;
