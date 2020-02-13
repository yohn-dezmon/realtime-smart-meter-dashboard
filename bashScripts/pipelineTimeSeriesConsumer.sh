#!/bin/bash

# (5) order to be run

# this is a pipeline to run all of the producers/streams/consumers
# this pipeline assumes that zookeeper/kafka/cassandra/redis are all running

# ensure that pegasus will work
. ~/.bashrc

# start up ssh
# eval `ssh-agent -s`
#
# peg fetch kafka

peg sshcmd-node kafka 3 java -jar /home/ubuntu/timeseries.jar
