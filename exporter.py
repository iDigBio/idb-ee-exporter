import os
import argparse
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import col, dayofmonth, lit, max, month, year

sc = SparkContext(appName="iDigBioEEExporter")
sqlContext = SQLContext(sc)

source_dir = "/guoda/data"
dest_dir = "/guoda/outputs"


argparser = argparse.ArgumentParser(description="iDigBio export for EE")
argparser.add_argument("--parquet", "-p", required=True, help="Parquet to export")
args = vars(argparser.parse_args())

par_fn = os.path.join(source_dir, args["parquet"])
exp_fn = os.path.join(dest_dir,
                      os.path.splitext(args["parquet"])[0] + "-ee.csv")
oneline_fn = os.path.join(dest_dir, exp_fn.replace("-ee.", "-ee-oneline."))


print(par_fn, exp_fn, oneline_fn)

# Have to turn on column case sensativity to deal with multiple verbatimEventDate 
# capitalizations. See https://redmine.idigbio.org/issues/2760
sqlContext.sql('set spark.sql.caseSensitive=true')


df = sqlContext.read.parquet(par_fn)


# Make list of references to all columns and all nested fields in struct columns
def easy_name(s):
    """Remove special characters in column names"""
    return s.replace(':', '_')

def escape_name(s):
    """Escape special characters in columns names"""
    return "`{0}`".format(s)


selects = []
for s in df.schema:
    name = str(s.name)
    dataType = str(s.dataType)
    if "StructType" in str(s.dataType):
        struct_name = str(s.name)
        for f in s.dataType:
            name = str(f.name)
            selects.append("{0}.{1} as {0}_{2}".format(easy_name(struct_name), 
                                                       escape_name(name), 
                                                        easy_name(name)))
    else:
        selects.append("{0} as {1}".format(escape_name(name), easy_name(name)))


# Flatten with the list of references to simplify things to a format that can be written to CSV
flat = df.selectExpr(selects)

(flat
 .coalesce(1)
 .write
 .format("com.databricks.spark.csv")
 .mode("overwrite")
 .option("header", "true")
 .save(exp_fn)
)

#(flat
# .limit(1)
# .coalesce(1)
# .write
# .format("com.databricks.spark.csv")
# .mode("overwrite")
# .option("header", "true")
# .save(oneline_fn)
#)
