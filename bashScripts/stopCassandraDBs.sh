#!/bin/bash

# Stop cassandra on each node (1,2,3):

# in order for ./stop-server to work you need to go into the stop-server file
# within the /bin directory and un-comment the two lines:
# 1 user=`whoami`
# 2 pgrep -u $user -f cassandra | xargs kill -9

. ~/.bashrc

peg sshcmd-cluster cassandra /home/ubuntu/apache-cassandra-3.11.5/bin/stop-server
