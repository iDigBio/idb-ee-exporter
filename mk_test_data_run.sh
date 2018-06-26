#!/bin/bash

export HADOOP_CONF_DIR="/etc/hadoop/conf"
export HADOOP_USER_NAME="hdfs"

/opt/spark/latest/bin/spark-submit \
             --driver-memory 256G \
             --total-executor-cores 120 \
             --conf spark.local.dir=/dev/shm \
             mk_test_data.py

#             --conf spark.sql.shuffle.partitions=3000 \
#             --master mesos://zk://mesos02:2181,mesos01:2181,mesos03:2181/mesos \
#              --executor-memory 20G \
 
