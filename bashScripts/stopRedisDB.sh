#!/bin/bash

. ~/.bashrc

peg sshcmd-node webserver 1 redis-cli shutdown SAVE
