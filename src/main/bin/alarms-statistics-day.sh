#!/bin/bash

conf=application.properties

sh dir.sh

baseDir=/data/can
daytime=`date -d "1 day ago" "+/%Y/%m/%d/"`
inputDir=${baseDir}${daytime}
outputBaseDir=/data/spark/alarmStatus/day/
durations=1800000

numPartitions=2
limit=1500

JAR_PATH=../lib/umltech-alarms-statistics-day-1.0.0.jar

MAIN_CLASS=com.umltech.DayStatusApp2

executor_cores=1

num_executors=1

executor_memory=2G

for jar in ../lib/*.jar
do
  if [ $jar != $JAR_PATH ] ; then
    LIBJARS=$jar,$LIBJARS
  fi
done

config=../conf/${conf}
appName=umltech-alarms-statistics-day

# --conf spark.streaming.kafka.maxRatePerPartition=10000 \

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class $MAIN_CLASS \
  --conf spark.app.name=${appName} \
  --jars $LIBJARS \
  --files $config \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.shuffle.memoryFraction=0.5 \
  --conf spark.storage.memoryFraction=0.2 \
  --conf spark.driver.memory=2G \
  --conf spark.shuffle.consolidateFiles=true \
  --executor-cores $executor_cores \
  --num-executors $num_executors \
  --executor-memory $executor_memory \
  --conf spark.streaming.backpressure.enabled=true \
  $JAR_PATH \
  $inputDir $outputBaseDir $numPartitions $limit $daytime
