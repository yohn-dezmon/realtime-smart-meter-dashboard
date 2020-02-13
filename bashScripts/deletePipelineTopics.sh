#!/bin/bash

# general script to delete a topic in kafka on the AWS cluster

# ensure that pegasus will work
. ~/.bashrc

# # start up ssh
# eval `ssh-agent -s`


# before running this, make sure you go into kafka 2 (peg ssh kafka 2)
# run: ps aux | grep java
# kill #### to kill any kafka streams apps that are still running

# peg sshcmd-node <cluster-name> <node-number> "<cmd>" - run a bash command on a specific node in your AWS cluster


# https://medium.com/@contactsunny/manually-delete-apache-kafka-topics-424c7e016ff3
# cd /usr/local/zookeeper/bin
# zkCli.sh
# get /brokers/topics/<topic_name>
# rmr /brokers/topics/<topic_name>
# get /brokers/topics/<topic_name> [[[should error]]]
# quit

# reset anomaly/moving avg kafka stream
peg sshcmd-node kafka 1 /usr/local/kafka/bin/kafka-streams-application-reset.sh --application-id anomaly-detector \
                                 --input-topics fake_iot \
                                     --intermediate-topics movingavg,theft,outage \
                                     --bootstrap-servers 127.0.0.1:9092 \
                                     --zookeeper zookeeperHost:2181

# resetting the cumulative sum kafka streams app
peg sshcmd-node kafka 1 /usr/local/kafka/bin/kafka-streams-application-reset.sh --application-id application \
                                 --input-topics fake_iot \
                                     --intermediate-topics cumulativesum \
                                     --bootstrap-servers 127.0.0.1:9092 \
                                     --zookeeper zookeeperHost:2181


peg sshcmd-node kafka 1 /usr/local/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic fake_iot --delete \
&
peg sshcmd-node kafka 1 /usr/local/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic cumulativesum --delete \
&
peg sshcmd-node kafka 2 /usr/local/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic movingavg --delete \
&
peg sshcmd-node kafka 2 /usr/local/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic outage --delete \
&
peg sshcmd-node kafka 2 /usr/local/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic theft --delete
