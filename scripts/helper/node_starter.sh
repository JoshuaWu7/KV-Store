#!/bin/bash

SERVER_COUNT=$1
START_PORT=$2
END_PORT=$(($START_PORT + $SERVER_COUNT - 1))

cd serverjar

echo "setting packet loss + delay"
sudo tc qdisc add dev lo root netem delay 5msec loss 2.5%
sudo tc qdisc add dev ens5 root netem delay 5msec loss 2.5%

for i in $(seq $START_PORT $END_PORT);
do
  echo $i
  java -Xmx64m \
  -XX:+UseCompressedOops \
  --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
  --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
  --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -jar A7.jar \
  $i &
done
echo "Server creation complete"
exit