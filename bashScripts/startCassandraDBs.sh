#!/bin/bash

# Start cassandra on each node (1,2,3):

. ~/.bashrc

peg sshcmd-cluster cassandra /home/ubuntu/apache-cassandra-3.11.5/bin/cassandra
