#!/bin/bash

# (7) order to be run



# ensure that pegasus will work
. ~/.bashrc


peg sshcmd-node kafka 3 java -jar /home/ubuntu/redissum.jar
