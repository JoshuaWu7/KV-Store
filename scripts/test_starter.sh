#!/bin/bash

AWS_HOST=$(head -n 1 aws_tester.txt)

echo "Setting up directory on remote"
ssh -o StrictHostKeyChecking=no $AWS_HOST "mkdir -p testjar"

echo "copying eval client to remote"
scp -o StrictHostKeyChecking=no \
../a6_eval.jar $AWS_HOST:~/testjar/a6_eval.jar

echo "copying server to remote"
scp -o StrictHostKeyChecking=no \
../target/CPEN431_2024_PROJECT_7-1.0-SNAPSHOT-jar-with-dependencies.jar $AWS_HOST:~/testjar/A6.jar

echo "copying servers.txt to remote"
echo "127.0.0.1:43100" | ssh $AWS_HOST 'cat > ~/testjar/servers.txt'

echo "copying full server list to remote"
scp -o StrictHostKeyChecking=no \
../servers.txt $AWS_HOST:~/testjar/servers_all.txt

echo "ssh to remote server"
cat single_starter.sh | ssh -o StrictHostKeyChecking=no $AWS_HOST

echo "start tester"
cat start_eval.sh | ssh -o StrictHostKeyChecking=no $AWS_HOST

echo "Test complete"


