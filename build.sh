#!/usr/bin/env bash
rm -r -f bin
mkdir -p bin
cd src
for f in *.java
do
    javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/hadoop/client-0.20/* $f
    jar cf ${f%%.*}.jar *.class
    rm -f *.class
    mv ${f%%.*}.jar ../bin
done
cd ..