#!/usr/bin/env bash
hadoop fs -rm -r -f /user/cloudera/input
hadoop fs -put input/input /user/cloudera/
rm -r -f output
mkdir -p output

cd bin
for javaFile in *
do
    hadoop fs -rm -f -r /user/cloudera/output
    hadoop jar $javaFile ${javaFile%%.*} /user/cloudera/input /user/cloudera/output
    hadoop fs -get /user/cloudera/${javaFile%%.*} ../output/
done
cd ..