*******************************************************************************;
* compute BMI and de-duplicate study records ;
* (from prepare-numom2b-for-nsrr.sas, lines 50-58) ;
*******************************************************************************;
  data sdb_vars_in;
    set sdb_vars_to_append;

    bmi = Weight / (Height/100 * Height/100);
  run;

  proc sort data=sdb_vars_in nodupkey;
    by SDB_StudyID stdydt stdyvis;
  run;

  proc print data=sdb_vars_in;
    var SDB_StudyID stdyvis Weight Height bmi;
  run;
