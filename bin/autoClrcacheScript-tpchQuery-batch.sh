#!/bin/sh
#use warm-up memory when query,
trap "exit" INT
for((j=3;j<23;j++))
do if [ $j -ne 11 -a $j -ne 21 ];then
> tpch-query-$j-output
> tpch-query-$j-output-time
         spark-submit --class TpchQuery --master spark://hw163:7077 tpchQuery.jar orc uncompressed query $j 3 >> tpch-query-$j-output
  cat tpch-query-$j-output | grep iteration >> tpch-query-$j-output-time
fi
done
