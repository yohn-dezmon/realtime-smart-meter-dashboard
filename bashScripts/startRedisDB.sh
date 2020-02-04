#!/bin/bash

. ~/.bashrc

peg sshcmd-node cassandra 1 redis-server --daemonize yes

# this needs to be done to allow writes from other nodes
# if this is successful, it will return 'OK'
peg sshcmd-node cassandra 1 redis-cli CONFIG SET protected-mode no
