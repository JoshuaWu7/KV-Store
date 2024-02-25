#!/bin/bash

AWS_HOST=$(head -n 1 aws_host.txt)

echo "ssh to remote server"
cat node_killer.sh | ssh -o StrictHostKeyChecking=no $AWS_HOST

echo "Servers killed"



