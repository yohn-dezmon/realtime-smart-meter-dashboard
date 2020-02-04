# Smart Meter Real Time Analytics
### Insight Data Engineering 2020
#### John Desmond

This is the main repository for my project using simulated smart meter data to create a real time dashboard allowing utility companies and their customers to analyze electricity data in real-time to save energy, detect outages, and suggest accurate and innovative pricing plans to customers.


## Table of Contents

1. [Purpose](https://github.com/yohn-dezmon/realtime-smart-meter-dashboard#purpose)
2. [Instructions for cloning and setting up project](https://github.com/yohn-dezmon/instructions-for-cloning-and-setting-up-project)
3. [Kafka Details](https://github.com/yohn-dezmon/realtime-smart-meter-dashboard#kafka-details)


## Purpose:  
The purpose of this pipeline is to process simulated electricity data from several thousand households into a single pipeline, in which real-time analytics can be applied to electricity data to benefit the utility companies and the customers.

## Instructions for cloning and setting up project:

### Creating clusters

To create the clusters I downloaded the [Pegasus](https://github.com/InsightDataScience/pegasus) package from Insight and followed the instructions found on the readme.

After installing pegasus, I created one cluster called 'kafka' consisting of three nodes. This assumes you have already set up security groups, subnets and a VPC on your AWS account.
Below is the .yml file I used by calling peg up kafka3.yml

```
purchase_type: on_demand
subnet_id: subnet-063e9c687f2b28604
num_instances: 3
key_name: John-Desmond-IAM-keypair
security_group_ids: sg-0e03c7fd4dcd32839
instance_type: m4.large
tag_name: kafka
vol_size: 100
role: master
use_eips: true
```
I also created a cluster of three nodes for Cassandra called 'cassandra' using another .yml file and the same peg command.

Finally I created a standalone node called webserver by running another .yml file and changing the tag_name to webserver and the num_instances to 1.

### Starting clusters

First, start your clusters:
```
$ peg start kafka
$ peg start cassandra
$ peg start webserver
```


### Installing Technologies

I used pegasus to install zookeeper and kafka (zookeeper must be installed to run kafka)
general syntax for pegasus: peg install <cluster-name> <technology>
:
```
$ peg install kafka zookeeper
$ peg install kafka kafka
```

I installed Cassandra manually on each node in the Cassandra cluster by ssh-ing into each node and following the directions [here](https://maelfabien.github.io/bigdata/EC2_Cassandra/#install-cassandra):

```
$ peg ssh cassandra 1
complete directions...
repeat for cassandra 2 and cassandra 3
```

I installed Redis on cassandra 1 using the directions found [here](https://maelfabien.github.io/bigdata/EC2_Cassandra/#install-cassandra).

### Starting up technologies


Start up zookeeper first, then kafka (wait a few minutes for zookeeper
to get up and running):
```
$ peg service kafka zookeeper start
...
$ peg service kafka kafka start

```

To start Cassandra and Redis, please go to the bashScripts folder within project, make the bash scripts executable ```chmod 755 *.sh``` and ensure that your ~/.bashrc or ~/.bashprofile is configured with your AWS access keys, then run the following:

```
$ ./startCassandraDBs.sh
$ ./startRedisDB.sh
```


The project consists of two clusters, one for Kafka and one for Cassandra.  
Zookeeper and Kafka and Java 8 must be installed on the instances on the Kafka Cluster. Once you have installed these technologies, you can clone this github repository to your EC2 instance.  
Before using the producers to pass in the data to Kafka, you should first create a topic called fake_iot.
The topic should consist of 6 partitions and a replication factor of at least two.    
To run the three Kafka producers in parallel, use the bashScripts/threeJavaProd.sh bash script.



## Kafka Details:

I created three Kafka producers, each of which generate a third of the simulated data. Each Kafka producer corresponds to a certain range of GPS coordinates, which are then converted into geohashes for their efficiency in storage (and also for graphing purposes). Each Kafka producer also produces a timestamp for a given set of values, and assigns an energy value to each row based on a normal distribution. Ultimately these data are supposed to represent 1 second interval readings from 10,000 homes in London, UK.
