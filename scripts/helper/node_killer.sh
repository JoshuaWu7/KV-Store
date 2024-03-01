#!/bin/bash

sudo tc qdisc del dev lo root
sudo tc qdisc del dev ens5 root


killall -9 java