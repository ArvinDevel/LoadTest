#!/bin/sh
#use warm-up memory when query,
#tpchQuery parameter defines
#orc/parquet compression load/query
#if query, it has two more parameters
#queryId #iter

> tpch-query-output
> tpch-query-output-time
  sync
  echo 3 > /proc/sys/vm/drop_caches
num=1
for((i=0;i<$num;i++)) 
do
  spark-submit --class TpchQuery --master spark://hw163:7077 tpchQuery.jar parquet default load >> tpch-query-output
#  sync
#  echo 3 > /proc/sys/vm/drop_caches
done
  cat tpch-query-output | grep time >> tpch-query-output-time
