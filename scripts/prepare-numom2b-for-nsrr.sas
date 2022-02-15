*******************************************************************************;
* Program           : prepare-numom2b-for-nsrr.sas
* Project           : National Sleep Research Resource (sleepdata.org)
* Author            : Michael Rueschman (mnr)
* Date Created      : 20191218
* Purpose           : Prepare nuMoM2b data for posting on NSRR.
*******************************************************************************;

*******************************************************************************;
* establish options and libnames ;
*******************************************************************************;
  options nofmterr;
  data _null_;
    call symput("sasfiledate",put(year("&sysdate"d),4.)||put(month("&sysdate"d),z2.)||put(day("&sysdate"d),z2.));
  run;

  *project source datasets;
  libname numoms "\\rfawin\bwh-sleepepi-numom2b\nsrr-prep\_source";

  *output location for nsrr sas datasets;
  libname numomd "\\rfawin\bwh-sleepepi-numom2b\nsrr-prep\_datasets";
  libname numoma "\\rfawin\bwh-sleepepi-numom2b\nsrr-prep\_archive";

  *nsrr id location;
  libname numomi "\\rfawin\bwh-sleepepi-numom2b\nsrr-prep\_ids";

  *set data dictionary version;
  %let version = 0.3.0;

  *set nsrr csv release path;
  %let releasepath = \\rfawin\bwh-sleepepi-numom2b\nsrr-prep\_releases;

*******************************************************************************;
* create datasets ;
*******************************************************************************;
  data numompsg_in;
    length SDB_StudyID $8. stdydt stdyvis 8.;
    set numoms.numompsgfull;

    SDB_StudyID = substr(pptid,1,8);

    *only keep passed sleep studies;
    if status = 1;
  run;

  proc sort data=numompsg_in nodupkey;
    by SDB_StudyID stdydt stdyvis;
  run;

  data sdb_vars_in;
    set numoms.sdb_vars_to_append;

    bmi = Weight / (Height/100 * Height/100);
  run;

  proc sort data=sdb_vars_in nodupkey;
    by SDB_StudyID stdydt stdyvis;
  run;

  *import baseline spo2 data;
  proc import datafile="\\rfawin\bwh-sleepepi-numom2b\nsrr-prep\_source\numom_baseline_spo2.csv"
    out=numom_baseline_spo2_in
    dbms=csv
    replace;
  run;

  data numom_baseline_spo2;
    set numom_baseline_spo2_in;

    SDB_StudyID = substr(numomid,1,8);
    stdyvis = input(substr(numomid,10,1),8.);

    if stdyvis = 5 then delete;

    keep SDB_StudyID stdyvis avgspo2baseline;
  run;

  proc sort data=numom_baseline_spo2 nodupkey;
    by SDB_StudyID stdyvis;
  run;

  *combine psg and baseline spo2;
  data numompsg_spo2;
    merge
      numompsg_in
      numom_baseline_spo2
      ;
    by SDB_StudyID stdyvis;
  run;

  /*

  *look at subjects whose StudyID changed during study;
  proc sql;
    select SDB_StudyID, StudyID
    from sdb_vars
    where SDB_StudyID ne Studyid and StudyID ne '';
  quit;

  */

  data numom_nsrr;
    merge
      sdb_vars_in (in=a)
      numompsg_spo2 (in=b)
      ;
    by SDB_StudyID stdydt stdyvis;

    *only keep subjects in both datasets with a PublicID and passed study;
    if a and b and PublicID ne '';
  run;

  proc sort data=numom_nsrr nodupkey;
    by PublicID stdyvis;
  run;

  data numom_nsrr_censored;
    set numom_nsrr;

    *rename variables;
    rename
      ahi_a0h0 = ahi_ap0nhp3x0n
      ahi_a0h3 = ahi_ap0nhp3x3n
      ahi_a0h4 = ahi_ap0nhp3x4n
      ahi_a0t4f3 = ahi_ap0nhp3x5x4nhp5x3n
      ahi_a2h2 = ahi_ap2nhp3x2n
      ahiu3 = ahi_ap3nhp3x3n
      ahi_a4h4 = ahi_ap4nhp3x4n
      ahi_a5h5 = ahi_ap5nhp3x5n
      cai_c0 = cai_ca0n
      cai_c4 = cai_ca4n
      cardnbp = cai_pb_ca0n
      cardnbp2 = cai_pb_ca2n
      cardnbp3 = cai_pb_ca3n
      cardnbp4 = cai_pb_ca4n
      cardnbp5 = cai_pb_ca5n
      cardnop = cai_po_ca0n
      cardnop2 = cai_po_ca2n
      cardnop3 = cai_po_ca3n
      cardnop4 = cai_po_ca4n
      cardnop5 = cai_po_ca5n
      hypi = hi_hp3x5x0n
      hypai = hi_hp5x0n
      rdinbp = hi_pb_hp3x0n
      rdinbp2 = hi_pb_hp3x2n
      rdinbp3 = hi_pb_hp3x3n
      rdinbp4 = hi_pb_hp3x4n
      rdinbp5 = hi_pb_hp3x5n
      rdinop = hi_po_hp3x0n
      rdinop2 = hi_po_hp3x2n
      rdinop3 = hi_po_hp3x3n
      rdinop4 = hi_po_hp3x4n
      rdinop5 = hi_po_hp3x5n
      oahi_o0h3 = oahi_oa0nhp3x3n
      oahi_o0h4 = oahi_oa0nhp3x4n
      oai_o0 = oai_oa0n
      oai_o4 = oai_oa4n
      oardnbp = oai_pb_oa0n
      oardnbp2 = oai_pb_oa2n
      oardnbp3 = oai_pb_oa3n
      oardnbp4 = oai_pb_oa4n
      oardnbp5 = oai_pb_oa5n
      oardnop = oai_po_oa0n
      oardnop2 = oai_po_oa2n
      oardnop3 = oai_po_oa3n
      oardnop4 = oai_po_oa4n
      oardnop5 = oai_po_oa5n
      bpmavg = avglvlhr
      avghrahslp = avglvlhr_ap0nhp3x0n
      savbnbh = avglvlhr_pb_dsgt0
      havbnbh = avglvlhr_pb_dsge3
      savbnoh = avglvlhr_po_dsgt0
      havbnoh = avglvlhr_po_dsge3
      bpmmax = maxlvlhr
      mxhrahslp = maxlvlhr_ap0nhp3x0n
      smxbnbh = maxlvlhr_pb_dsgt0
      hmxbnbh = maxlvlhr_pb_dsge3
      smxbnoh = maxlvlhr_po_dsgt0
      hmxbnoh = maxlvlhr_po_dsge3
      bpmmin = minlvlhr
      mnhrahslp = minlvlhr_ap0nhp3x0n
      smnbnbh = minlvlhr_pb_dsgt0
      hmnbnbh = minlvlhr_pb_dsge3
      smnbnoh = minlvlhr_po_dsgt0
      hmnbnoh = minlvlhr_po_dsge3
      avdnbp = avgdurds_pb_dsgt0
      avdnbp2 = avgdurds_pb_dsge2
      avdnbp3 = avgdurds_pb_dsge3
      avdnbp4 = avgdurds_pb_dsge4
      avdnbp5 = avgdurds_pb_dsge5
      avdnop = avgdurds_po_dsgt0
      avdnop2 = avgdurds_po_dsge2
      avdnop3 = avgdurds_po_dsge3
      avdnop4 = avgdurds_po_dsge4
      avdnop5 = avgdurds_po_dsge5
      avgdsslp = avglvlds
      avgdsevent = avglvlds_ap0nhp3x0n
      saondnrem = avglvlnd
      saondcaslp = avglvlnd_ca0n
      saondoaslp = avglvlnd_oa0n
      avsao2nh = avglvlsa
      mxdnbp = maxlvlds_pb_dsgt0
      mxdnbp2 = maxlvlds_pb_dsge2
      mxdnbp3 = maxlvlds_pb_dsge3
      mxdnbp4 = maxlvlds_pb_dsge4
      mxdnbp5 = maxlvlds_pb_dsge5
      mxdnop = maxlvlds_po_dsgt0
      mxdnop2 = maxlvlds_po_dsge2
      mxdnop3 = maxlvlds_po_dsge3
      mxdnop4 = maxlvlds_po_dsge4
      mxdnop5 = maxlvlds_po_dsge5
      mxsao2nh = maxlvlsa
      minsaondnrem = minlvlnd
      minsaondcaslp = minlvlnd_ca0n
      minsaondoaslp = minlvlnd_oa0n
      lowsaoslp = minlvlsa
      mndnbp = minlvlsa_pb_dsgt0
      mndnbp2 = minlvlsa_pb_dsge2
      mndnbp3 = minlvlsa_pb_dsge3
      mndnbp4 = minlvlsa_pb_dsge4
      mndnbp5 = minlvlsa_pb_dsge5
      mndnop = minlvlsa_po_dsgt0
      mndnop2 = minlvlsa_po_dsge2
      mndnop3 = minlvlsa_po_dsge3
      mndnop4 = minlvlsa_po_dsge4
      mndnop5 = minlvlsa_po_dsge5
      pctlt75 = pctdursp_salt75
      pctlt80 = pctdursp_salt80
      pctlt85 = pctdursp_salt85
      pctlt90 = pctdursp_salt90
      desati3 = phrnumds_dsge3
      desati4 = phrnumds_dsge4
      sao92slp = ttldursa_sagt92
      sao90awk = ttldursa_sw_salt90
      sao92awk = ttldursa_sw_sagt92
      ndesat3 = ttlnumds_su_dsge3
      ndesat4 = ttlnumds_su_dsge4
      ndes2ph = ttlnumds_so_dsge2
      ndes3ph = ttlnumds_so_dsge3
      ndes4ph = ttlnumds_so_dsge4
      ndes5ph = ttlnumds_so_dsge5
      dssao90 = ttlnumds_salt90
      hunrbp = hi_pb_hp5x0n
      hunrbp2 = hi_pb_hp5x2n
      hunrbp3 = hi_pb_hp5x3n
      hunrbp4 = hi_pb_hp5x4n
      hunrbp5 = hi_pb_hp5x5n
      hunrop = hi_po_hp5x0n
      hunrop2 = hi_po_hp5x2n
      hunrop3 = hi_po_hp5x3n
      hunrop4 = hi_po_hp5x4n
      hunrop5 = hi_po_hp5x5n
      canbp = ttlnumca_pb_ca0n
      canbp2 = ttlnumca_pb_ca2n
      canbp3 = ttlnumca_pb_ca3n
      canbp4 = ttlnumca_pb_ca4n
      canbp5 = ttlnumca_pb_ca5n
      canop = ttlnumca_po_ca0n
      canop2 = ttlnumca_po_ca2n
      canop3 = ttlnumca_po_ca3n
      canop4 = ttlnumca_po_ca4n
      canop5 = ttlnumca_po_ca5n
      hnrbp = ttlnumhp_pb_hp3x5x0n
      hnrbp2 = ttlnumhp_pb_hp3x5x2n
      hnrbp3 = ttlnumhp_pb_hp3x5x3n
      hnrbp4 = ttlnumhp_pb_hp3x5x4n
      hnrbp5 = ttlnumhp_pb_hp3x5x5n
      unrbp = ttlnumhp_pb_hp5x0n
      unrbp2 = ttlnumhp_pb_hp5x2n
      unrbp3 = ttlnumhp_pb_hp5x3n
      unrbp4 = ttlnumhp_pb_hp5x4n
      unrbp5 = ttlnumhp_pb_hp5x5n
      hnrop = ttlnumhp_po_hp3x5x0n
      hnrop2 = ttlnumhp_po_hp3x5x2n
      hnrop3 = ttlnumhp_po_hp3x5x3n
      hnrop4 = ttlnumhp_po_hp3x5x4n
      hnrop5 = ttlnumhp_po_hp3x5x5n
      unrop = ttlnumhp_po_hp5x0n
      unrop2 = ttlnumhp_po_hp5x2n
      unrop3 = ttlnumhp_po_hp5x3n
      unrop4 = ttlnumhp_po_hp5x4n
      unrop5 = ttlnumhp_po_hp5x5n
      oanbp = ttlnumoa_pb_oa0n
      oanbp2 = ttlnumoa_pb_oa2n
      oanbp3 = ttlnumoa_pb_oa3n
      oanbp4 = ttlnumoa_pb_oa4n
      oanbp5 = ttlnumoa_pb_oa5n
      oanop = ttlnumoa_po_oa0n
      oanop2 = ttlnumoa_po_oa2n
      oanop3 = ttlnumoa_po_oa3n
      oanop4 = ttlnumoa_po_oa4n
      oanop5 = ttlnumoa_po_oa5n
      apavgdur = avgdurap_ap0n
      cavgdur = avgdurca_ca0n
      avcanbp = avgdurca_pb_ca0n
      avcanbp2 = avgdurca_pb_ca2n
      avcanbp3 = avgdurca_pb_ca3n
      avcanbp4 = avgdurca_pb_ca4n
      avcanbp5 = avgdurca_pb_ca5n
      avcanop = avgdurca_po_ca0n
      avcanop2 = avgdurca_po_ca2n
      avcanop3 = avgdurca_po_ca3n
      avcanop4 = avgdurca_po_ca4n
      avcanop5 = avgdurca_po_ca5n
      hbavgdur = avgdurhp_hp3x0n
      avhnbp = avgdurhp_pb_hp3x5x0n
      avhnbp2 = avgdurhp_pb_hp3x5x2n
      avhnbp3 = avgdurhp_pb_hp3x5x3n
      avhnbp4 = avgdurhp_pb_hp3x5x4n
      avhnbp5 = avgdurhp_pb_hp3x5x5n
      avunrbp = avgdurhp_pb_hp5x0n
      avunrbp2 = avgdurhp_pb_hp5x2n
      avunrbp3 = avgdurhp_pb_hp5x3n
      avunrbp4 = avgdurhp_pb_hp5x4n
      avunrbp5 = avgdurhp_pb_hp5x5n
      avhnop = avgdurhp_po_hp3x5x0n
      avhnop2 = avgdurhp_po_hp3x5x2n
      avhnop3 = avgdurhp_po_hp3x5x3n
      avhnop4 = avgdurhp_po_hp3x5x4n
      avhnop5 = avgdurhp_po_hp3x5x5n
      avunrop = avgdurhp_po_hp5x0n
      avunrop2 = avgdurhp_po_hp5x2n
      avunrop3 = avgdurhp_po_hp5x3n
      avunrop4 = avgdurhp_po_hp5x4n
      avunrop5 = avgdurhp_po_hp5x5n
      oavgdur = avgduroa_oa0n
      avoanbp = avgduroa_pb_oa0n
      avoanbp2 = avgduroa_pb_oa2n
      avoanbp3 = avgduroa_pb_oa3n
      avoanbp4 = avgduroa_pb_oa4n
      avoanbp5 = avgduroa_pb_oa5n
      avoanop = avgduroa_po_oa0n
      avoanop2 = avgduroa_po_oa2n
      avoanop3 = avgduroa_po_oa3n
      avoanop4 = avgduroa_po_oa4n
      avoanop5 = avgduroa_po_oa5n
      longap = maxdurap_ap0n
      longhypb = maxdurap_hp3x0n
      mxcanbp = maxdurca_pb_ca0n
      mxcanbp2 = maxdurca_pb_ca2n
      mxcanbp3 = maxdurca_pb_ca3n
      mxcanbp4 = maxdurca_pb_ca4n
      mxcanbp5 = maxdurca_pb_ca5n
      mxcanop = maxdurca_po_ca0n
      mxcanop2 = maxdurca_po_ca2n
      mxcanop3 = maxdurca_po_ca3n
      mxcanop4 = maxdurca_po_ca4n
      mxcanop5 = maxdurca_po_ca5n
      mxhnbp = maxdurhp_pb_hp3x5x0n
      mxhnbp2 = maxdurhp_pb_hp3x5x2n
      mxhnbp3 = maxdurhp_pb_hp3x5x3n
      mxhnbp4 = maxdurhp_pb_hp3x5x4n
      mxhnbp5 = maxdurhp_pb_hp3x5x5n
      lunrbp = maxdurhp_pb_hp5x0n
      lunrbp2 = maxdurhp_pb_hp5x2n
      lunrbp3 = maxdurhp_pb_hp5x3n
      lunrbp4 = maxdurhp_pb_hp5x4n
      lunrbp5 = maxdurhp_pb_hp5x5n
      mxhnop = maxdurhp_po_hp3x5x0n
      mxhnop2 = maxdurhp_po_hp3x5x2n
      mxhnop3 = maxdurhp_po_hp3x5x3n
      mxhnop4 = maxdurhp_po_hp3x5x4n
      mxhnop5 = maxdurhp_po_hp3x5x5n
      lunrop = maxdurhp_po_hp5x0n
      lunrop2 = maxdurhp_po_hp5x2n
      lunrop3 = maxdurhp_po_hp5x3n
      lunrop4 = maxdurhp_po_hp5x4n
      lunrop5 = maxdurhp_po_hp5x5n
      mxoanbp = maxduroa_pb_oa0n
      mxoanbp2 = maxduroa_pb_oa2n
      mxoanbp3 = maxduroa_pb_oa3n
      mxoanbp4 = maxduroa_pb_oa4n
      mxoanbp5 = maxduroa_pb_oa5n
      mxoanop = maxduroa_po_oa0n
      mxoanop2 = maxduroa_po_oa2n
      mxoanop3 = maxduroa_po_oa3n
      mxoanop4 = maxduroa_po_oa4n
      mxoanop5 = maxduroa_po_oa5n
      mncanbp = mindurca_pb_ca0n
      mncanbp2 = mindurca_pb_ca2n
      mncanbp3 = mindurca_pb_ca3n
      mncanbp4 = mindurca_pb_ca4n
      mncanbp5 = mindurca_pb_ca5n
      mncanop = mindurca_po_ca0n
      mncanop2 = mindurca_po_ca2n
      mncanop3 = mindurca_po_ca3n
      mncanop4 = mindurca_po_ca4n
      mncanop5 = mindurca_po_ca5n
      mnhnbp = mindurhp_pb_hp3x5x0n
      mnhnbp2 = mindurhp_pb_hp3x5x2n
      mnhnbp3 = mindurhp_pb_hp3x5x3n
      mnhnbp4 = mindurhp_pb_hp3x5x4n
      mnhnbp5 = mindurhp_pb_hp3x5x5n
      sunrbp = mindurhp_pb_hp5x0n
      sunrbp2 = mindurhp_pb_hp5x2n
      sunrbp3 = mindurhp_pb_hp5x3n
      sunrbp4 = mindurhp_pb_hp5x4n
      sunrbp5 = mindurhp_pb_hp5x5n
      mnhnop = mindurhp_po_hp3x5x0n
      mnhnop2 = mindurhp_po_hp3x5x2n
      mnhnop3 = mindurhp_po_hp3x5x3n
      mnhnop4 = mindurhp_po_hp3x5x4n
      mnhnop5 = mindurhp_po_hp3x5x5n
      sunrop = mindurhp_po_hp5x0n
      sunrop2 = mindurhp_po_hp5x2n
      sunrop3 = mindurhp_po_hp5x3n
      sunrop4 = mindurhp_po_hp5x4n
      sunrop5 = mindurhp_po_hp5x5n
      mnoanbp = minduroa_pb_oa0n
      mnoanbp2 = minduroa_pb_oa2n
      mnoanbp3 = minduroa_pb_oa3n
      mnoanbp4 = minduroa_pb_oa4n
      mnoanbp5 = minduroa_pb_oa5n
      mnoanop = minduroa_po_oa0n
      mnoanop2 = minduroa_po_oa2n
      mnoanop3 = minduroa_po_oa3n
      mnoanop4 = minduroa_po_oa4n
      mnoanop5 = minduroa_po_oa5n
      pcoslp = pctdursp_ap0n
      pcohb3slp = pctdursp_ap3nhp3x3n
      phbslp = pctdursp_hp3x0n
      ahntdur = ttldurah_ap0nhp3x0n
      ahntdurbp = ttldurah_pb_ap0nhp3x0n
      ahntdurop = ttldurah_po_ap0nhp3x0n
      apntdur = ttldurap_ap0n
      apntdurbp = ttldurap_pb_ap0n
      ctdur = ttldurca_ca0n
      cntdurbp = ttldurca_pb_ca0n
      cntdurop = ttldurca_po_ca0n
      htdur = ttldurhp_hp3x5x0n
      hntdurbp = ttldurhp_pb_hp3x5x0n
      hntdurop = ttldurhp_po_hp3x5x0n
      aptdur = ttldurma_ma0n
      ontdur = ttlduroa_oa0n
      ontdurbp = ttlduroa_pb_oa0n
      ontdurop = ttlduroa_po_oa0n
      stlonp = endtimbd
      stendp = endtimrd
      supinep = pctdursp_pb
      nsupinep = pctdursp_po
      hslptawp = phrnumsf
      slp_eff = ttleffbd
      slplatp = ttllatsp
      slp_maint_eff = ttleffsp
      stloutp = begtimbd
      ststartp = begtimrd
      stonsetp = begtimsp
      slpprdp = ttldursp
      nremepbp = ttldursp_pb
      nremepop = ttldursp_po
      time_bed = ttlprdbd
      stdurm = ttlprdrd
      slpprdm = ttlprdsp
      waso = ttldurws
      pdb5slp = pctdursp_se
      soundi = phrnumsd
      nsound = ttlnumsd
      ;

    *censor variables;
    drop
      SDB_StudyID /* private identifier */
      StudyID /* private identifier */
      stdydt /* date of psg recording - no dates */
      V1AF05 -- V1AF07g /* individual race categories - only keep combined race variable */
      pptid /* private identifier */
      siteid /* private identifier */
      rcvddt /* date received - no dates */
      staendt /* date week ending - no dates */
      reviewdt /* date of review - no dates */
      techid /* private identifier */
      status /* only passed studies kept */
      rsnco /* failure reason - all missing since only passed studies kept */
      pfcomm /* free text comments about sleep study */
      scordt /* date scored - no dates */
      scordtwkend /* date scored week ending - no dates */
      urgalert_notifydt /* date urgent alert notified - no dates */
      urgalert_replydt /* date urgent alert acknowledged - no dates */
      overall_comments /* free text comments about sleep study */
      STDATEP /* date of psg study - no dates */
      SCOREDT /* date psg scored - no dates */
      consentflag /* all subjects retained have consentflag = 1 */

      /* remaining variables redundant or extraneous */
      ahi_a3h3
      ahins_a0h0
      ahins_a2h2
      ahins_a3h3
      ahins_a4h4
      ahins_a5h5
      ahis_a0h0
      ahis_a2h2
      ahis_a3h3
      ahis_a4h4
      ahis_a5h5
      apnea3
      avgdsresp
      avgsaominrpt
      avgsaominslp
      avgsat
      cai
      cntdur
      dsnr2
      dsnr3
      dsnr4
      dsnr5
      hntdur
      minsat
      mnsao2nh
      oai
      otdur
      pctsa70h
      pctsa75h
      pctsa80h
      pctsa85h
      pctsa90h
      pctsa95h
      slpeffp
      slplatm
      slptimem
      timebedm
      timebedp
      wasom
      ;
  run;

  proc sort data=numom_nsrr_censored nodupkey;
    by PublicID stdyvis;
  run;

*******************************************************************************;
* rename variables with suffix ;
*******************************************************************************;
  proc sql noprint;
     select cats(name,'=',name,'_f1t3')
            into :list
            separated by ' '
            from dictionary.columns
            where libname = 'WORK' and memname = 'NUMOM_NSRR_CENSORED';
  quit;

  proc datasets library = work nolist;
     modify numom_nsrr_censored;
     rename &list;
  quit;

  data numom_nsrr_censored;
    set numom_nsrr_censored;

    rename
      stdyvis_f1t3 = stdyvis
      height_f1t3 = height
      age_at_stdydt_f1t3 = age_at_stdydt
      bmi_f1t3 = bmi
      crace_f1t3 = crace
      ga_at_stdydt_f1t3 = ga_at_stdydt
      publicid_f1t3 = publicid
      weight_f1t3 = weight
      ;
  run;

*******************************************************************************;
* create separate datasets for each visit ;
*******************************************************************************;
  data numom_nsrr_visit1 numom_nsrr_visit3;
    set numom_nsrr_censored;

    if stdyvis = 1 then output numom_nsrr_visit1;
    else if stdyvis = 3 then output numom_nsrr_visit3;
  run;

*******************************************************************************;
* create harmonized datasets ;
*******************************************************************************;
data numom_nsrr_visit1_harmonized;
  set numom_nsrr_visit1;
  where stdyvis=1;
*demographics
*age;
*use age_at_stdydt;
  format nsrr_age 8.2;
  nsrr_age = age_at_stdydt;

*age_gt89;
*use age_at_stdydt;
  format nsrr_age_gt89 $100.; 
  if age_at_stdydt gt 89 then nsrr_age_gt89='yes';
  else if age_at_stdydt le 89 then nsrr_age_gt89='no';

*sex;
*create nsrr_sex all female;
  format nsrr_sex $100.;
  nsrr_sex = 'female';

*race;
*use crace;
    format nsrr_race $100.;
    if crace = 1 then nsrr_race = 'white';
    else if crace = 2 then nsrr_race = 'black or african american';
    else if crace = 3 then nsrr_race = 'hispanic';
    else if crace = 4 then nsrr_race = 'asian';
  else if crace = 5 then nsrr_race = 'other';
  else  nsrr_race = 'not reported';

*ethnicity;
*use crace;
  format nsrr_ethnicity $100.;
    if crace = 3 then nsrr_ethnicity = 'hispanic or latino';
    else if crace = 1 then nsrr_ethnicity = 'not hispanic or latino';
  else if crace = 2  then nsrr_ethnicity = 'not hispanic or latino';
  else if crace = 4   then nsrr_ethnicity = 'not hispanic or latino';
  else if crace = 5  then nsrr_ethnicity = 'not hispanic or latino';
  else if crace = . then nsrr_ethnicity = 'not reported';

*anthropometry
*bmi;
*use bmi;
  format nsrr_bmi 10.9;
  nsrr_bmi = bmi;

*clinical data/vital signs
*bp_systolic;
*bp_diastolic;
  *not available;

*lifestyle and behavioral health
*current_smoker;
*ever_smoker;
  *not available;

*polysomnography;
*nsrr_ahi_hp3u;
*use ahi_ap0nhp3x3n_f1t3;
  format nsrr_ahi_hp3u 8.2;
  nsrr_ahi_hp3u = ahi_ap0nhp3x3n_f1t3;
  
*nsrr_ahi_hp4u_aasm15;
*use ahi_ap0nhp3x4n_f1t3;
  format nsrr_ahi_hp4u_aasm15 8.2;
  nsrr_ahi_hp4u_aasm15 = ahi_ap0nhp3x4n_f1t3;

*nsrr_ttldursp_f1;
*use ttldursp_f1t3;
  format nsrr_ttldursp_f1 8.2;
  nsrr_ttldursp_f1 = ttldursp_f1t3;
  
  keep 
    publicid
    stdyvis
    nsrr_age
    nsrr_age_gt89
    nsrr_sex
    nsrr_race
    nsrr_ethnicity
    nsrr_bmi
	nsrr_ahi_hp3u
	nsrr_ahi_hp4u_aasm15
	nsrr_ttldursp_f1
    ;
run;

*******************************************************************************;
* checking harmonized datasets ;
*******************************************************************************;

/* Checking for extreme values for continuous variables */

proc means data=numom_nsrr_visit1_harmonized;
VAR   nsrr_age
    nsrr_bmi
	nsrr_ahi_hp3u
	nsrr_ahi_hp4u_aasm15
	nsrr_ttldursp_f1
	;
run;

/* Checking categorical variables */

proc freq data=numom_nsrr_visit1_harmonized;
table   nsrr_age_gt89
    nsrr_sex
    nsrr_race
    nsrr_ethnicity;
run;


*******************************************************************************;
* make all variable names lowercase ;
*******************************************************************************;
  options mprint;
  %macro lowcase(dsn);
       %let dsid=%sysfunc(open(&dsn));
       %let num=%sysfunc(attrn(&dsid,nvars));
       %put &num;
       data &dsn;
             set &dsn(rename=(
          %do i = 1 %to &num;
          %let var&i=%sysfunc(varname(&dsid,&i));    /*function of varname returns the name of a SAS data set variable*/
          &&var&i=%sysfunc(lowcase(&&var&i))         /*rename all variables*/
          %end;));
          %let close=%sysfunc(close(&dsid));
    run;
  %mend lowcase;

  %lowcase(numom_nsrr_censored);
  %lowcase(numom_nsrr_visit1);
  %lowcase(numom_nsrr_visit3);
  %lowcase(numom_nsrr_visit1_harmonized);

  /*

  proc contents data=numom_nsrr_censored out=numom_nsrr_contents;
  run;

  */



*******************************************************************************;
* create permanent sas datasets ;
*******************************************************************************;
  data numomd.numomvisit1 numoma.numomvisit1_&sasfiledate;
    set numom_nsrr_visit1;
  run;

  data numomd.numomvisit3 numoma.numomvisit3_&sasfiledate;;
    set numom_nsrr_visit3;
  run;

*******************************************************************************;
* export nsrr csv datasets ;
*******************************************************************************;
  proc export data=numom_nsrr_visit1
    outfile="&releasepath\&version\numom-visit1-dataset-&version..csv"
    dbms=csv
    replace;
  run;

  proc export data=numom_nsrr_visit3
    outfile="&releasepath\&version\numom-visit3-dataset-&version..csv"
    dbms=csv
    replace;
  run;

    proc export data=numom_nsrr_visit1_harmonized
    outfile="&releasepath\&version\numom-visit1-harmonized-&version..csv"
    dbms=csv
    replace;
  run;
