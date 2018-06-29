#!/bin/bash

export HADOOP_CONF_DIR="/etc/hadoop/conf"
export HADOOP_USER_NAME="hdfs"

/opt/spark/latest/bin/spark-submit \
    --master mesos://zk://mesos02:2181,mesos01:2181,mesos03:2181/mesos \
    --driver-memory 2G \
    --total-executor-cores 80 \
    --executor-memory 20G \
    differ.py --end idigbio-20180519T023311-1Msorted.parquet --start idigbio-20180414T023309-1Msorted.parquet
