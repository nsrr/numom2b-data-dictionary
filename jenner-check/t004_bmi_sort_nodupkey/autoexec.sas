options obs=100;

/* Stand-in for the upstream numoms.sdb_vars_to_append source dataset,
   built only from the columns the BMI/sort step reads. Two rows are
   duplicated so the NODUPKEY sort has records to remove. Inlined as
   DATALINES so the bundle is self-contained. */
data sdb_vars_to_append;
  input SDB_StudyID stdydt stdyvis PublicID Weight Height crace age_at_stdydt;
  datalines;
10000001 21001 1 200001 68.4 165.0 1 27
10000002 21002 1 200002 82.1 172.5 2 31
10000003 21003 1 200003 55.9 158.0 3 22
10000004 21004 3 200004 95.3 168.0 4 38
10000005 21005 1 200005 71.2 170.0 5 29
10000005 21005 1 200005 71.2 170.0 5 29
10000006 21006 3 200006 88.0 175.5 1 34
10000007 21007 1 200007 60.5 160.5 2 26
10000008 21008 3 200008 99.8 166.0 3 41
10000008 21008 3 200008 99.8 166.0 3 41
10000009 21009 1 200009 57.3 162.0 4 24
10000010 21010 3 200010 80.6 171.0 5 33
;
run;
