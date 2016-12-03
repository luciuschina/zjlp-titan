#!/bin/bash

SPARK_APP_HOME=/opt/sparkApp/zjlp-titan   # zjlp-titan-standby
declare -a arr=("11" "12" "13")
for i in "${arr[@]}"
do
scp -r $SPARK_APP_HOME/config.properties root@192.168.175.$i:$SPARK_HOME/conf/
scp -r $SPARK_APP_HOME/titan-cassandra.properties  root@192.168.175.$i:$SPARK_APP_HOME/titan-cassandra.properties
done

spark-submit --class com.zjlp.face.spark.base.DataMigration --driver-java-options -DPropPath=$SPARK_APP_HOME/config.properties --jars /opt/sparkApp/mysql-connector-java-5.1.22.jar $SPARK_APP_HOME/zjlp-titan-1.0.jar