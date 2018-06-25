from __future__ import print_function
from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types

sc = SparkContext(appName="iDigBioParquet")
sqlContext = SQLContext(sc)

(sqlContext.read.parquet("/guoda/data/idigbio-20180414T023309.parquet")
    .orderBy("uuid")
    .limit(1000 * 1000)
    .write
    .mode("overwrite")
    .parquet("/tmp/idigbio-20180414T023309-sorted1M.parquet")
)


