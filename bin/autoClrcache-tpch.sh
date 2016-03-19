#!/bin/sh
#need execute as root to execute clrCache action

#take care of space character in shell.
> tpch-output
> tpch-output-time
  sync
  echo 3 > /proc/sys/vm/drop_caches
num=4
for((i=0;i<$num;i++))
do
  spark-submit --class TpchLoadPerformance --master spark://hw163:7077 tpch.jar part parquet off >> tpch-output
  sync
  echo 3 > /proc/sys/vm/drop_caches
done
  cat tpch-output | grep time >> tpch-output-time
#spark-submit --class TpchLoadPerformance --master spark://hw163:7077 tpch.jar lineitem orc off
#sync
#echo 3 > /proc/sys/vm/drop_caches