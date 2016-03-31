#!/bin/sh
#man:first parameter as part of the output file name
> tpch-query-orc-$1-output
for((j=1;j<23;j++))
do
  echo query-$j >> tpch-query-orc-$1-output
  cat tpch-query-$j-output | grep Iteration >> tpch-query-orc-$1-output
  cat tpch-query-$j-output | grep time >> tpch-query-orc-$1-output
  echo -e >> tpch-query-orc-$1-output
done
