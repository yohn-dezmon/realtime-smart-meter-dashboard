#!/bin/bash

# general script to create kafka topics on the AWS cluster

# ensure that pegasus will work
. ~/.bashrc

# start up ssh
# eval `ssh-agent -s`


# peg sshcmd-node <cluster-name> <node-number> "<cmd>" - run a bash command on a specific node in your AWS cluster

peg sshcmd-node kafka 1 /usr/local/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic fake_iot --create --partitions 3 --replication-factor 3

peg sshcmd-node kafka 1 /usr/local/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic movingavg --create --partitions 1 --replication-factor 3

peg sshcmd-node kafka 1 /usr/local/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic outage --create --partitions 1 --replication-factor 3

peg sshcmd-node kafka 1 /usr/local/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic cumulativesum --create --partitions 1 --replication-factor 3

peg sshcmd-node kafka 1 /usr/local/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic theft --create --partitions 1 --replication-factor 3
