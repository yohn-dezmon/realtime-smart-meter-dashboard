#!/bin/bash

# (1) order to be run

# ensure that pegasus will work
. ~/.bashrc

# start up ssh
# eval `ssh-agent -s`

# this assumes all topics have been deleted then recreated as blank topics
# this script also assumes that the kafka streams apps have been reset
peg sshcmd-node kafka 1 java -jar /home/ubuntu/dataGenerator.jar

peg peg sshcmd-node kafka 1 java -jar /home/ubuntu/dataGenerator2.jar

peg sshcmd-node kafka 1 java -jar /home/ubuntu/dataGenerator3.jar
# echo PRODUCER LAUNCHED
