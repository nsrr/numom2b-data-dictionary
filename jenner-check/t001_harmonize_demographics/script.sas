*******************************************************************************;
* create harmonized datasets ;
* (harmonization DATA step from prepare-numom2b-for-nsrr.sas, lines 575-660) ;
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

proc print data=numom_nsrr_visit1_harmonized;
run;
