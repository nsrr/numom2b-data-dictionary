*******************************************************************************;
* checking harmonized datasets ;
* (PROC MEANS QC step from prepare-numom2b-for-nsrr.sas, lines 668-675) ;
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
