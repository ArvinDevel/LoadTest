#!/bin/sh

  sync
  echo 3 > /proc/sys/vm/drop_caches
> netflow-output
num=4
for((i=0;i<$num;i++))
do
  spark-submit --class NetflowLoadPerformance --master spark://hw163:7077 netflowTest.jar netflow orc off >> netflow-output
  sync
  echo 3 > /proc/sys/vm/drop_caches
done
cat netflow-output | grep time > netflow-output-time
