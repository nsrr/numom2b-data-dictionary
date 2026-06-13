options obs=100;

/* Stand-in for the upstream numom_nsrr_visit1 working dataset, built only
   from the columns the harmonization step reads. Inlined as DATALINES so
   the bundle is self-contained. */
data numom_nsrr_visit1;
  input publicid stdyvis age_at_stdydt bmi crace
        ahi_ap0nhp3x3n_f1t3 ahi_ap0nhp3x4n_f1t3 ttldursp_f1t3;
  datalines;
100001 1 27 24.531234567 1 3.21 1.04 412.5
100002 1 31 28.902345678 2 7.85 4.10 388.0
100003 1 22 21.115678901 3 1.02 0.41 455.0
100004 1 38 33.447890123 4 12.44 8.92 360.5
100005 1 29 26.778901234 5 5.67 3.18 401.0
100006 1 34 30.221345678 1 9.10 6.05 375.5
100007 1 26 23.889012345 2 2.55 1.20 430.0
100008 1 41 35.992345678 3 15.30 11.22 342.0
100009 1 24 22.334567890 4 0.95 0.30 448.5
100010 1 33 29.556789012 5 8.40 5.55 392.0
100011 1 28 25.110234567 1 4.12 2.05 408.0
100012 1 36 31.778901234 2 11.05 7.40 368.5
;
run;

/* harmonization DATA step from prepare-numom2b-for-nsrr.sas (lines 575-660),
   producing numom_nsrr_visit1_harmonized for the downstream QC procs. */
data numom_nsrr_visit1_harmonized;
  set numom_nsrr_visit1;
  where stdyvis=1;
  format nsrr_age 8.2;
  nsrr_age = age_at_stdydt;

  format nsrr_age_gt89 $100.;
  if age_at_stdydt gt 89 then nsrr_age_gt89='yes';
  else if age_at_stdydt le 89 then nsrr_age_gt89='no';

  format nsrr_sex $100.;
  nsrr_sex = 'female';

  format nsrr_race $100.;
  if crace = 1 then nsrr_race = 'white';
  else if crace = 2 then nsrr_race = 'black or african american';
  else if crace = 3 then nsrr_race = 'hispanic';
  else if crace = 4 then nsrr_race = 'asian';
  else if crace = 5 then nsrr_race = 'other';
  else  nsrr_race = 'not reported';

  format nsrr_ethnicity $100.;
  if crace = 3 then nsrr_ethnicity = 'hispanic or latino';
  else if crace = 1 then nsrr_ethnicity = 'not hispanic or latino';
  else if crace = 2  then nsrr_ethnicity = 'not hispanic or latino';
  else if crace = 4   then nsrr_ethnicity = 'not hispanic or latino';
  else if crace = 5  then nsrr_ethnicity = 'not hispanic or latino';
  else if crace = . then nsrr_ethnicity = 'not reported';

  format nsrr_bmi 10.9;
  nsrr_bmi = bmi;

  format nsrr_ahi_hp3u 8.2;
  nsrr_ahi_hp3u = ahi_ap0nhp3x3n_f1t3;

  format nsrr_ahi_hp4u_aasm15 8.2;
  nsrr_ahi_hp4u_aasm15 = ahi_ap0nhp3x4n_f1t3;

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
