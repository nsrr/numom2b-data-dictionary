*******************************************************************************;
* create separate datasets for each visit ;
* (from prepare-numom2b-for-nsrr.sas, lines 565-570) ;
*******************************************************************************;
  data numom_nsrr_visit1 numom_nsrr_visit3;
    set numom_nsrr_censored;

    if stdyvis = 1 then output numom_nsrr_visit1;
    else if stdyvis = 3 then output numom_nsrr_visit3;
  run;

  proc print data=numom_nsrr_visit1;
    var publicid stdyvis age_at_stdydt bmi;
  run;

  proc print data=numom_nsrr_visit3;
    var publicid stdyvis age_at_stdydt bmi;
  run;
