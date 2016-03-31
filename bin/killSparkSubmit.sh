#!/bin/sh
for i in 1 2 3 4 5 6
do sleep 2s
pid=`jps | grep SparkSubmit | cut -d ' ' -f 1`
kill $pid
done
