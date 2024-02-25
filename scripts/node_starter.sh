#!/bin/bash

SERVER_COUNT=4
START_PORT=13788
END_PORT=$(($START_PORT + $SERVER_COUNT - 1))

for i in $(seq $START_PORT $END_PORT);
do
  echo $i
  java \
  -XX:+UseCompressedOops \
  -Xmx64m \
  --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
  --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
  --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -jar target/CPEN431_2024_PROJECT_7-1.0-SNAPSHOT-jar-with-dependencies.jar \
  $i &
done
