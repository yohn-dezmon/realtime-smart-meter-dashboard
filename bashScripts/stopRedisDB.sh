#!/bin/bash

. ~/.bashrc

peg sshcmd-node cassandra 1 redis-cli shutdown
