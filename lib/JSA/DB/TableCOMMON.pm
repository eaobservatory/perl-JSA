package  JSA::DB::TableCOMMON;

use strict; use warnings;

my $_name = 'COMMON';

sub table { return $_name; }

sub column_names {

  return
    qw[
        align_dx align_dy alt_obs amend amstart atend atstart azend
        backend bklegten bklegtst bpend bpstart
        date_end date_obs daz dut1
        etal
        focus_dz frlegten frlegtst
        hstend hststart humend humstart
        instap instap_x instap_y instrume
        jos_min jos_mult
        last_modified lat_obs locl_crd long_obs
        map_hght map_pa map_wdth map_x map_y msbid msbtid
        num_cyc
        object obs_type
        obsdec obsdecbl obsdecbr obsdectl obsdectr obsgeo_x obsgeo_y obsgeo_z
        obsid obsnum obsra obsrabl obsrabr obsratl obsratr ocscfg origin
        project
        recipe release_date
        sam_mode scan_crd scan_dy scan_pa scan_pat scan_vel standard startidx
        status steptime sw_mode
        tau225en tau225st taudaten taudatst tausrc telescop
        uaz uel utdate
        wnddiren wnddirst wndspden wndspdst wvmdaten wvmdatst wvmtauen wvmtaust
      ];
}

sub date_columns {

  return
    qw[
        date_obs date_end
        hststart hstend
        release_date
        taudatst taudaten
        wvmdatst wvmdaten
        seedatst seedaten
      ];
}


1;


