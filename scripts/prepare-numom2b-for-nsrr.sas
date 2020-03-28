*******************************************************************************;
* Program           : prepare-numom2b-for-nsrr.sas
* Project           : National Sleep Research Resource (sleepdata.org)
* Author            : Michael Rueschman (mnr)
* Date Created      : 20191218
* Purpose           : Prepare nuMoM2b data for posting on NSRR.
* Revision History  :
*   Date      Author    Revision
*   
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
