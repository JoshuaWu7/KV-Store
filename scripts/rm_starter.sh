#!/bin/bash

AWS_HOST=$(head -n 1 aws_host.txt)

echo "Setting up directory on remote"
ssh -o StrictHostKeyChecking=no $AWS_HOST "mkdir -p serverjar"

echo "copying jar to remote"
scp -o StrictHostKeyChecking=no \
../target/CPEN431_2024_PROJECT_7-1.0-SNAPSHOT-jar-with-dependencies.jar $AWS_HOST:~/serverjar/A6.jar

echo "copying servers.txt to remote"
scp -o StrictHostKeyChecking=no \
../servers.txt $AWS_HOST:~/serverjar/servers.txt

echo "ssh to remote server"
cat ./helper/node_starter.sh | ssh -o StrictHostKeyChecking=no $AWS_HOST

echo "Servers started"



