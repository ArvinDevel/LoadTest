#!/bin/sh

> random-output
> random-output-time
  sync
  echo 3 > /proc/sys/vm/drop_caches
num=4
for((i=0;i<$num;i++))
do
  spark-submit --class WritePerformance --master spark://hw163:7077 sparkAppContainMap.jar double orc doubleFile.txt >> random-output
  sync
  echo 3 > /proc/sys/vm/drop_caches
done
  cat random-output | grep time >> random-output-time