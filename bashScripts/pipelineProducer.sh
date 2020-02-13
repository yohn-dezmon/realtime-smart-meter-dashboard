#!/bin/bash

# (1) order to be run

# this is a pipeline to run all of the producers/streams/consumers
# this pipeline assumes that zookeeper/kafka/cassandra/redis are all running

# ensure that pegasus will work
. ~/.bashrc

# start up ssh
# eval `ssh-agent -s`

# this assumes all topics have been deleted then recreated as blank topics
# this script also assumes that the kafka streams apps have been reset
peg sshcmd-node kafka 1 java -jar /home/ubuntu/dataGenerator.jar
# echo PRODUCER LAUNCHED
