#!/bin/bash

AWS_HOST=$(head -n 1 aws_host.txt)
PRIV_IP=$(head -n 1 private_ip.txt)
START_PORT=$(sed -n '2p' node_setup.txt)
SERVER_COUNT=$(sed -n '1p' node_setup.txt)
END_PORT=$(($START_PORT + $SERVER_COUNT - 1))

echo "Setting up directory on remote"
ssh -o StrictHostKeyChecking=no $AWS_HOST "mkdir -p serverjar"

echo "copying jar to remote"
scp -o StrictHostKeyChecking=no \
../target/CPEN431_2024_PROJECT_7-1.0-SNAPSHOT-jar-with-dependencies.jar $AWS_HOST:~/serverjar/A7.jar

echo "copying servers.txt to remote"
> servers_all_tmp.txt
for i in $(seq $START_PORT $END_PORT);
do
  echo "$PRIV_IP:$i" >> servers_all_tmp.txt
done
scp -o StrictHostKeyChecking=no \
servers_all_tmp.txt $AWS_HOST:~/serverjar/servers.txt
rm servers_all_tmp.txt

echo "ssh to remote server"
ssh $AWS_HOST "bash -s $SERVER_COUNT $START_PORT" < ./helper/node_starter.sh

echo "Servers started"



