#!/bin/bash

# ensure that pegasus will work
. ~/.bashrc


# list all topics
peg sshcmd-node kafka 1 /usr/local/kafka/bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
