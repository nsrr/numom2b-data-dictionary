*******************************************************************************;
* checking harmonized datasets ;
* (PROC FREQ QC step from prepare-numom2b-for-nsrr.sas, lines 679-684) ;
*******************************************************************************;

/* Checking categorical variables */

proc freq data=numom_nsrr_visit1_harmonized;
table   nsrr_age_gt89
    nsrr_sex
    nsrr_race
    nsrr_ethnicity;
run;
