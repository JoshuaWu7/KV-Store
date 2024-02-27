#!/bin/bash

echo "starting test"
cd testjar
java -jar a6_eval.jar --submit --servers-list servers_all.txt --secret-code 2706426568

#java -jar a6_eval.jar --servers-list servers_all.txt


echo "killing all java"
killall -9 java