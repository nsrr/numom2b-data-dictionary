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
  %let version = 0.1.0.beta1;

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
      numompsg_in (in=b)
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
      ahi_a0h0 = ahi_ap0uhp3x0u
      ahi_a0h3 = ahi_ap0uhp3x3u
      ahi_a0h4 = ahi_ap0uhp3x4u
      ahi_a0t4f3 = ahi_ap0uhp3x5x4uhp5x3u
      ahi_a2h2 = ahi_ap2uhp3x2u
      ahiu3 = ahi_ap3uhp3x3u
      ahi_a4h4 = ahi_ap4uhp3x4u
      ahi_a5h5 = ahi_ap5uhp3x5u
      cai_c0 = cai_ca0u
      cai_c4 = cai_ca4u
      cardnbp = cai_pb_ca0u
      cardnbp2 = cai_pb_ca2u
      cardnbp3 = cai_pb_ca3u
      cardnbp4 = cai_pb_ca4u
      cardnbp5 = cai_pb_ca5u
      cardnop = cai_po_ca0u
      cardnop2 = cai_po_ca2u
      cardnop3 = cai_po_ca3u
      cardnop4 = cai_po_ca4u
      cardnop5 = cai_po_ca5u
      hypi = hi_hp3x5x0u
      hypai = hi_hp5x0u
      rdinbp = hi_pb_hp3x0u
      rdinbp2 = hi_pb_hp3x2u
      rdinbp3 = hi_pb_hp3x3u
      rdinbp4 = hi_pb_hp3x4u
      rdinbp5 = hi_pb_hp3x5u
      rdinop = hi_po_hp3x0u
      rdinop2 = hi_po_hp3x2u
      rdinop3 = hi_po_hp3x3u
      rdinop4 = hi_po_hp3x4u
      rdinop5 = hi_po_hp3x5u
      oahi_o0h3 = oahi_oa3uhp3x3u
      oahi_o0h4 = oahi_oa4uhp3x4u
      oai_o0 = oai_oa0u
      oai_o4 = oai_oa4u
      oardnbp = oai_pb_oa0u
      oardnbp2 = oai_pb_oa2u
      oardnbp3 = oai_pb_oa3u
      oardnbp4 = oai_pb_oa4u
      oardnbp5 = oai_pb_oa5u
      oardnop = oai_po_oa0u
      oardnop2 = oai_po_oa2u
      oardnop3 = oai_po_oa3u
      oardnop4 = oai_po_oa4u
      oardnop5 = oai_po_oa5u
      bpmavg = avglvlhr
      avghrahslp = avglvlhr_ap0uhp3x0u
      savbnbh = avglvlhr_pbd0
      havbnbh = avglvlhr_pbd3
      savbnoh = avglvlhr_pod0
      havbnoh = avglvlhr_pod3
      bpmmax = maxlvlhr
      mxhrahslp = maxlvlhr_ap0uhp3x0u
      smxbnbh = maxlvlhr_pbd0
      hmxbnbh = maxlvlhr_pbd3
      smxbnoh = maxlvlhr_pod0
      hmxbnoh = maxlvlhr_pod3
      bpmmin = minlvlhr
      mnhrahslp = minlvlhr_ap0uhp3x0u
      smnbnbh = minlvlhr_pbd0
      hmnbnbh = minlvlhr_pbd3
      smnbnoh = minlvlhr_pod0
      hmnbnoh = minlvlhr_pod3
      avdnbp = avgdurds_pb_ds0u
      avdnbp2 = avgdurds_pb_ds2u
      avdnbp3 = avgdurds_pb_ds3u
      avdnbp4 = avgdurds_pb_ds4u
      avdnbp5 = avgdurds_pb_ds5u
      avdnop = avgdurds_po_ds0u
      avdnop2 = avgdurds_po_ds2u
      avdnop3 = avgdurds_po_ds3u
      avdnop4 = avgdurds_po_ds4u
      avdnop5 = avgdurds_po_ds5u
      avgdsslp = avglvlds
      avgdsevent = avglvlds_ap0uhp3x0u
      saondnrem = avglvlnd
      saondcaslp = avglvlnd_ca0u
      saondoaslp = avglvlnd_oa0u
      avsao2nh = avglvlsa
      mxdnbp = maxlvlds_pbd0
      mxdnbp2 = maxlvlds_pbd2
      mxdnbp3 = maxlvlds_pbd3
      mxdnbp4 = maxlvlds_pbd4
      mxdnbp5 = maxlvlds_pbd5
      mxdnop = maxlvlds_pod0
      mxdnop2 = maxlvlds_pod2
      mxdnop3 = maxlvlds_pod3
      mxdnop4 = maxlvlds_pod4
      mxdnop5 = maxlvlds_pod5
      mxsao2nh = maxlvlsa
      minsaondnrem = minlvlnd
      minsaondcaslp = minlvlnd_ca0u
      minsaondoaslp = minlvlnd_oa0u
      lowsaoslp = minlvlsa
      mndnbp = minlvlsa_pbd0
      mndnbp2 = minlvlsa_pbd2
      mndnbp3 = minlvlsa_pbd3
      mndnbp4 = minlvlsa_pbd4
      mndnbp5 = minlvlsa_pbd5
      mndnop = minlvlsa_pod0
      mndnop2 = minlvlsa_pod2
      mndnop3 = minlvlsa_pod3
      mndnop4 = minlvlsa_pod4
      mndnop5 = minlvlsa_pod5
      pctlt75 = pctdursp_o75
      pctlt80 = pctdursp_o80
      pctlt85 = pctdursp_o85
      pctlt90 = pctdursp_o90
      desati3 = phrnumds_ds3
      desati4 = phrnumds_ds4
      sao92slp = ttldursat_o92
      sao90awk = ttldursat_swo90
      sao92awk = ttldursat_swo92
      ndesat3 = ttlnumds_d3
      ndesat4 = ttlnumds_d4
      ndes2ph = ttlnumds_ds2u
      ndes3ph = ttlnumds_ds3u
      ndes4ph = ttlnumds_ds4u
      ndes5ph = ttlnumds_ds5u
      dssao90 = ttlnumds_o90
      hunrbp = hi_pb_hp5x0u
      hunrbp2 = hi_pb_hp5x2u
      hunrbp3 = hi_pb_hp5x3u
      hunrbp4 = hi_pb_hp5x4u
      hunrbp5 = hi_pb_hp5x5u
      hunrop = hi_po_hp5x0u
      hunrop2 = hi_po_hp5x2u
      hunrop3 = hi_po_hp5x3u
      hunrop4 = hi_po_hp5x4u
      hunrop5 = hi_po_hp5x5u
      canbp = ttlnumca_pb_ca0u
      canbp2 = ttlnumca_pb_ca2u
      canbp3 = ttlnumca_pb_ca3u
      canbp4 = ttlnumca_pb_ca4u
      canbp5 = ttlnumca_pb_ca5u
      canop = ttlnumca_po_ca0u
      canop2 = ttlnumca_po_ca2u
      canop3 = ttlnumca_po_ca3u
      canop4 = ttlnumca_po_ca4u
      canop5 = ttlnumca_po_ca5u
      hnrbp = ttlnumhp_pb_hp3x5x0u
      hnrbp2 = ttlnumhp_pb_hp3x5x2u
      hnrbp3 = ttlnumhp_pb_hp3x5x3u
      hnrbp4 = ttlnumhp_pb_hp3x5x4u
      hnrbp5 = ttlnumhp_pb_hp3x5x5u
      unrbp = ttlnumhp_pb_hp5x0u
      unrbp2 = ttlnumhp_pb_hp5x2u
      unrbp3 = ttlnumhp_pb_hp5x3u
      unrbp4 = ttlnumhp_pb_hp5x4u
      unrbp5 = ttlnumhp_pb_hp5x5u
      hnrop = ttlnumhp_po_hp3x5x0u
      hnrop2 = ttlnumhp_po_hp3x5x2u
      hnrop3 = ttlnumhp_po_hp3x5x3u
      hnrop4 = ttlnumhp_po_hp3x5x4u
      hnrop5 = ttlnumhp_po_hp3x5x5u
      unrop = ttlnumhp_po_hp5x0u
      unrop2 = ttlnumhp_po_hp5x2u
      unrop3 = ttlnumhp_po_hp5x3u
      unrop4 = ttlnumhp_po_hp5x4u
      unrop5 = ttlnumhp_po_hp5x5u
      oanbp = ttlnumoa_pb_oa0u
      oanbp2 = ttlnumoa_pb_oa2u
      oanbp3 = ttlnumoa_pb_oa3u
      oanbp4 = ttlnumoa_pb_oa4u
      oanbp5 = ttlnumoa_pb_oa5u
      oanop = ttlnumoa_po_oa0u
      oanop2 = ttlnumoa_po_oa2u
      oanop3 = ttlnumoa_po_oa3u
      oanop4 = ttlnumoa_po_oa4u
      oanop5 = ttlnumoa_po_oa5u
      apavgdur = avgdurap_ap0u
      cavgdur = avgdurca_ca0u
      avcanbp = avgdurca_pb_ca0u
      avcanbp2 = avgdurca_pb_ca2u
      avcanbp3 = avgdurca_pb_ca3u
      avcanbp4 = avgdurca_pb_ca4u
      avcanbp5 = avgdurca_pb_ca5u
      avcanop = avgdurca_po_ca0u
      avcanop2 = avgdurca_po_ca2u
      avcanop3 = avgdurca_po_ca3u
      avcanop4 = avgdurca_po_ca4u
      avcanop5 = avgdurca_po_ca5u
      hbavgdur = avgdurhp_hp3x0u
      avhnbp = avgdurhp_pb_hp3x5x0u
      avhnbp2 = avgdurhp_pb_hp3x5x2u
      avhnbp3 = avgdurhp_pb_hp3x5x3u
      avhnbp4 = avgdurhp_pb_hp3x5x4u
      avhnbp5 = avgdurhp_pb_hp3x5x5u
      avunrbp = avgdurhp_pb_hp5x0u
      avunrbp2 = avgdurhp_pb_hp5x2u
      avunrbp3 = avgdurhp_pb_hp5x3u
      avunrbp4 = avgdurhp_pb_hp5x4u
      avunrbp5 = avgdurhp_pb_hp5x5u
      avhnop = avgdurhp_po_hp3x5x0u
      avhnop2 = avgdurhp_po_hp3x5x2u
      avhnop3 = avgdurhp_po_hp3x5x3u
      avhnop4 = avgdurhp_po_hp3x5x4u
      avhnop5 = avgdurhp_po_hp3x5x5u
      avunrop = avgdurhp_po_hp5x0u
      avunrop2 = avgdurhp_po_hp5x2u
      avunrop3 = avgdurhp_po_hp5x3u
      avunrop4 = avgdurhp_po_hp5x4u
      avunrop5 = avgdurhp_po_hp5x5u
      oavgdur = avgduroa_oa0u
      avoanbp = avgduroa_pb_oa0u
      avoanbp2 = avgduroa_pb_oa2u
      avoanbp3 = avgduroa_pb_oa3u
      avoanbp4 = avgduroa_pb_oa4u
      avoanbp5 = avgduroa_pb_oa5u
      avoanop = avgduroa_po_oa0u
      avoanop2 = avgduroa_po_oa2u
      avoanop3 = avgduroa_po_oa3u
      avoanop4 = avgduroa_po_oa4u
      avoanop5 = avgduroa_po_oa5u
      longap = maxdurap_ap0u
      longhypb = maxdurap_hp3x0u
      mxcanbp = maxdurca_pb_ca0u
      mxcanbp2 = maxdurca_pb_ca2u
      mxcanbp3 = maxdurca_pb_ca3u
      mxcanbp4 = maxdurca_pb_ca4u
      mxcanbp5 = maxdurca_pb_ca5u
      mxcanop = maxdurca_po_ca0u
      mxcanop2 = maxdurca_po_ca2u
      mxcanop3 = maxdurca_po_ca3u
      mxcanop4 = maxdurca_po_ca4u
      mxcanop5 = maxdurca_po_ca5u
      mxhnbp = maxdurhp_pb_hp3x5x0u
      mxhnbp2 = maxdurhp_pb_hp3x5x2u
      mxhnbp3 = maxdurhp_pb_hp3x5x3u
      mxhnbp4 = maxdurhp_pb_hp3x5x4u
      mxhnbp5 = maxdurhp_pb_hp3x5x5u
      lunrbp = maxdurhp_pb_hp5x0u
      lunrbp2 = maxdurhp_pb_hp5x2u
      lunrbp3 = maxdurhp_pb_hp5x3u
      lunrbp4 = maxdurhp_pb_hp5x4u
      lunrbp5 = maxdurhp_pb_hp5x5u
      mxhnop = maxdurhp_po_hp3x5x0u
      mxhnop2 = maxdurhp_po_hp3x5x2u
      mxhnop3 = maxdurhp_po_hp3x5x3u
      mxhnop4 = maxdurhp_po_hp3x5x4u
      mxhnop5 = maxdurhp_po_hp3x5x5u
      lunrop = maxdurhp_po_hp5x0u
      lunrop2 = maxdurhp_po_hp5x2u
      lunrop3 = maxdurhp_po_hp5x3u
      lunrop4 = maxdurhp_po_hp5x4u
      lunrop5 = maxdurhp_po_hp5x5u
      mxoanbp = maxduroa_pb_oa0u
      mxoanbp2 = maxduroa_pb_oa2u
      mxoanbp3 = maxduroa_pb_oa3u
      mxoanbp4 = maxduroa_pb_oa4u
      mxoanbp5 = maxduroa_pb_oa5u
      mxoanop = maxduroa_po_oa0u
      mxoanop2 = maxduroa_po_oa2u
      mxoanop3 = maxduroa_po_oa3u
      mxoanop4 = maxduroa_po_oa4u
      mxoanop5 = maxduroa_po_oa5u
      mncanbp = mindurca_pb_ca0u
      mncanbp2 = mindurca_pb_ca2u
      mncanbp3 = mindurca_pb_ca3u
      mncanbp4 = mindurca_pb_ca4u
      mncanbp5 = mindurca_pb_ca5u
      mncanop = mindurca_po_ca0u
      mncanop2 = mindurca_po_ca2u
      mncanop3 = mindurca_po_ca3u
      mncanop4 = mindurca_po_ca4u
      mncanop5 = mindurca_po_ca5u
      mnhnbp = mindurhp_pb_hp3x5x0u
      mnhnbp2 = mindurhp_pb_hp3x5x2u
      mnhnbp3 = mindurhp_pb_hp3x5x3u
      mnhnbp4 = mindurhp_pb_hp3x5x4u
      mnhnbp5 = mindurhp_pb_hp3x5x5u
      sunrbp = mindurhp_pb_hp5x0u
      sunrbp2 = mindurhp_pb_hp5x2u
      sunrbp3 = mindurhp_pb_hp5x3u
      sunrbp4 = mindurhp_pb_hp5x4u
      sunrbp5 = mindurhp_pb_hp5x5u
      mnhnop = mindurhp_po_hp3x5x0u
      mnhnop2 = mindurhp_po_hp3x5x2u
      mnhnop3 = mindurhp_po_hp3x5x3u
      mnhnop4 = mindurhp_po_hp3x5x4u
      mnhnop5 = mindurhp_po_hp3x5x5u
      sunrop = mindurhp_po_hp5x0u
      sunrop2 = mindurhp_po_hp5x2u
      sunrop3 = mindurhp_po_hp5x3u
      sunrop4 = mindurhp_po_hp5x4u
      sunrop5 = mindurhp_po_hp5x5u
      mnoanbp = minduroa_pb_oa0u
      mnoanbp2 = minduroa_pb_oa2u
      mnoanbp3 = minduroa_pb_oa3u
      mnoanbp4 = minduroa_pb_oa4u
      mnoanbp5 = minduroa_pb_oa5u
      mnoanop = minduroa_po_oa0u
      mnoanop2 = minduroa_po_oa2u
      mnoanop3 = minduroa_po_oa3u
      mnoanop4 = minduroa_po_oa4u
      mnoanop5 = minduroa_po_oa5u
      pcoslp = pctdursp_ap0u
      pcohb3slp = pctdursp_ap3uhp3x3u
      phbslp = pctdursp_hp3x0u
      ahntdur = ttldurah_ap0uhp3x0u
      ahntdurbp = ttldurah_pb_ap0uhp3x0u
      ahntdurop = ttldurah_po_ap0uhp3x0u
      apntdur = ttldurap_ap0u
      apntdurbp = ttldurap_pb_ap0u
      ctdur = ttldurca_ca0u
      cntdurbp = ttldurca_pb_ca0u
      cntdurop = ttldurca_po_ca0u
      htdur = ttldurhp_hp3x5x0u
      hntdurbp = ttldurhp_pb_hp3x5x0u
      hntdurop = ttldurhp_po_hp3x5x0u
      aptdur = ttldurma_ma0u
      ontdur = ttlduroa_oa0u
      ontdurbp = ttlduroa_pb_oa0u
      ontdurop = ttlduroa_po_oa0u
      stlonp = endbd
      stendp = endrd
      supinep = pctdursp_pb
      nsupinep = pctdursp_po
      hslptawp = phrnumsf
      slp_eff = slpeff
      slplatp = slplatency
      slp_maint_eff = slpmaineff
      stloutp = startbd
      ststartp = startrd
      stonsetp = startsp
      slpprdp = ttldursp
      nremepbp = ttldursp_pb
      nremepop = ttldursp_po
      time_bed = ttlprdbd
      stdurm = ttlprdrd
      slpprdm = ttlprdsp
      duration = waso
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
      ahis_a1h1
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

  /*

  proc contents data=numom_nsrr_censored out=numom_nsrr_contents;
  run;

  */

*******************************************************************************;
* create separate datasets for each visit ;
*******************************************************************************;
  data numom_nsrr_visit1 numom_nsrr_visit3;
    set numom_nsrr_censored;

    if stdyvis = 1 then output numom_nsrr_visit1;
    else if stdyvis = 3 then output numom_nsrr_visit3;
  run;

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
