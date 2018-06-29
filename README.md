# Code for exporting data sets for Earth Engine

EE wants differences/incremental data. Use CSV as an interchance format, limit fields to what's in the index, 
drop verbatim fields that are too long. Diffs/patches of CSVs are probably not useful since that helps construct 
a whole snapshot. What they likely want is records flagged as new/change/del so they can read them and apply 
their data update process to each. (Would be cool for our own explorations too.)

File naming
* idigbio-datetime1-ee.csv.bz2 as the base full dump at a specific time
* idigbio-datetime1-datetime2-ee-diff.csv.bz2 as the first diff between T1 full and T2 full
* idigbio-datetime2-ee.csv.bz2 as the base full dump at second time

Each diff can be applied in order to move the data version up to the next one. Any full copy can be used as a starting point. The datemodified field is discontinious enough that it should be easy to tell what full snapshot you're currently on.


## Code

1. Exporter - Given a full dump parquet, format the data for EE and write out a slimmed .parquet
1. Differ - given two full exports, generate a list of records that are different and add a column that is 
new/change/del to indicate the operation needed, write out a .parquet
1. Checker - Given a starting full export, sequence of diffs, and a final export, verify the diffs yeild the final
1. Publisher - Move a .parquet into a csv on Ceph


## Attribution

How do people using EE know who is responsible for providing the data they're looking at?

## Data documentation
