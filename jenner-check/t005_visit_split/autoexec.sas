options obs=100;

/* Stand-in for the upstream numom_nsrr_censored working dataset, with a
   mix of Visit 1 and Visit 3 records so the split sends rows to both
   output datasets. Inlined as DATALINES so the bundle is self-contained. */
data numom_nsrr_censored;
  input publicid stdyvis age_at_stdydt bmi crace ahi_ap0nhp3x3n_f1t3 ttldursp_f1t3;
  datalines;
200001 1 27 25.12 1 3.21 412.5
200002 1 31 27.59 2 7.85 388.0
200003 1 22 22.39 3 1.02 455.0
200004 3 38 33.77 4 12.44 360.5
200005 1 29 24.64 5 5.67 401.0
200006 3 34 28.57 1 9.10 375.5
200007 1 26 23.49 2 2.55 430.0
200008 3 41 36.22 3 15.30 342.0
200009 1 24 21.83 4 0.95 448.5
200010 3 33 27.56 5 8.40 392.0
;
run;
