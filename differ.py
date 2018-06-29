import os
import argparse
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import col, dayofmonth, lit, max, month, year

sc = SparkContext(appName="iDigBioDiffer")
sqlContext = SQLContext(sc)

source_dir = "/outputs"
#source_dir = "/guoda/data"
dest_dir = "/guoda/outputs"


argparser = argparse.ArgumentParser(description="Difference two iDigBio parquet dumps.")
argparser.add_argument("--start", "-s", required=True, help="Earliest dump")
argparser.add_argument("--end", "-e", required=True, help="Later dump")
args = vars(argparser.parse_args())

# Could be better
t1_fn = os.path.join(source_dir, args["start"])
t2_fn = os.path.join(source_dir, args["end"])
t1_time = os.path.splitext(t1_fn)[0].split('-')[1]
t2_time = os.path.splitext(t2_fn)[0].split('-')[1]
diff_fn = os.path.join(dest_dir, "idigbio-{0}-{1}.parquet".format(t1_time, t2_time))

print(t1_time, t2_time, diff_fn)


t1_df = (sqlContext
         .read
         .parquet(t1_fn)
         .withColumnRenamed("uuid", "t1_uuid")
         )

t2_df = (sqlContext
         .read
         .parquet(t2_fn)
         .withColumnRenamed("uuid", "t2_uuid")
         )

last_updated = (t1_df
               .select(max(col("datemodified")).alias("last_updated"))
               .collect()
                )[0]["last_updated"]

added = (t2_df
           .join(t1_df.select(col("t1_uuid")),
                col("t1_uuid") == col("t2_uuid"), how="left")
           .filter(col("t1_uuid").isNull())
           .withColumn("op", lit('add'))
           )

deleted = (t1_df
           .join(t2_df.select(col("t2_uuid")),
                col("t1_uuid") == col("t2_uuid"), how="left")
           .filter(col("t2_uuid").isNull())
           .withColumn("op", lit('delete'))
           )

only_updated = (t2_df
                .filter((t2_df.datemodified > last_updated))
                .join(added.select(col("t2_uuid").alias("added_uuid")), 
                      col("t2_uuid") == col("added_uuid"), how="left")
                .filter(col("added_uuid").isNull())
                .withColumn("op", lit('update'))
                )

(added
    .withColumnRenamed("t2_uuid", "uuid").drop("t1_uuid")
    .union(deleted.withColumnRenamed("t1_uuid", "uuid").drop("t2_uuid"))
    .union(only_updated.withColumnRenamed("t2_uuid", "uuid").drop("added_uuid"))
    .write
    .mode('overwrite')
    .parquet(diff_fn)
)
