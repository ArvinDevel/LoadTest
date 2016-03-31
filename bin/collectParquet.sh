#!/bin/sh
> tpch-query-parquet-$1-output
for((j=1;j<23;j++))
do if [ $j -ne 11 ];then
  echo query-$j >> tpch-query-parquet-$1-output
  cat tpch-query-$j-output | grep Iteration >> tpch-query-parquet-$1-output
  cat tpch-query-$j-output | grep time >> tpch-query-parquet-$1-output
  echo -e >> tpch-query-parquet-$1-output
fi
done
