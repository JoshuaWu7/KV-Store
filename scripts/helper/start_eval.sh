#!/bin/bash

echo "starting test"
cd testjar
java -jar a7_eval.jar --servers-list=servers_all.txt --submit --secret-code 2706426568

#java -jar a7_eval.jar --servers-list servers_all.txt


echo "killing all java"
killall -9 java