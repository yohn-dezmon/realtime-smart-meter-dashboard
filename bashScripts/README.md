### Starting up technologies


Start up zookeeper first, then Kafka (wait a few minutes for zookeeper
to get up and running):
```
$ peg service kafka zookeeper start
...wait a few minutes...
$ peg service kafka kafka start

```

To start Cassandra and Redis, please go to the bashScripts folder within project, make the bash scripts executable ```chmod 755 *.sh``` and ensure that your ~/.bashrc or ~/.bashprofile is configured with your AWS access keys, then run the following:

```
$ cd bashScripts
$ ./startCassandraDBs.sh
$ ./startRedisDB.sh
```

### Running the Pipeline

Create all topics:
```
$ cd bashScripts
$ ./createPipelineTopics.sh
```

Now run the pipeline bash scripts in this order:
1. ./pipelineProducer.sh
2.
```
$ peg ssh kafka 2
$ java -jar /home/ubuntu/movingavgstream.jar
(this is necessary b/c ctrl-c needs to shut down the application
for the reset in deletePipelineTopics to be effective)
```
3.
```
$ peg ssh kafka 2
$ java -jar /home/ubuntu/jsonsum.jar
(!! This shouldn't produce any output except for initial connection to kafka brokers !!)
```

4. ./pipelineTimeSeriesConsumer.sh
5. ./pipelineMovingAvgConsumer.sh
6. ./pipelineOutageConsumer.sh
7. ./pipelineTheftConsumer.sh
8. ./pipelineSumRedisConsumer.sh

By doing so, will being putting data into the Cassandra and Redis tables.

### Running the Dash WebApp

Please see the README within the dash-webapp directory.

### Shutting Down AWS Cluster

The following commands can be run from your local host within this subdirectory (bashScripts)
to shut down your AWS cluster.

Stopping redis:
```
$ ./stopRedisDB.sh
```

Stopping Cassandra DBs:
in order for the bash script below to work you need to go into the stop-server file
within the /bin directory of Cassandra and uncomment the two lines:
(1) user=`whoami`
(2) pgrep -u $user -f cassandra | xargs kill -9

Now run the following bash script from local host:
```
$ ./stopCassandraDBs.sh
```

'kafka' is the name of my kafka cluster.
The general syntax for using peg to shutdown services is:
peg service <cluster-name> <service> stop
```
$ peg service kafka kafka stop
$ peg service kafka zookeeper stop
```

Stopping clusters (my cluster names are kafka and cassandra):
```
$ peg stop kafka
$ peg stop cassandra
$ peg stop webserver
```

If you ever need to delete the tables from Cassandra, follow the instructions below:  

1. SSH into the cassandra 1 EC2 instance
2. for <private-ip> enter your own private ip address for your EC2 instance
```
$ cd apache-cassandra-3.11.5/bin/
$ ./cqlsh <private-ip>
cqlsh> use geotime;
cqlsh:geotime> describe tables;
indivtimeseries  simpletimeseries
cqlsh:geotime> DROP TABLE indivtimeseries ;
cqlsh:geotime> DROP TABLE simpletimeseries ;
cqlsh:geotime> exit
```

If you ever need to delete the sorted sets from Redis, follow the instructions below:  

1. SSH into the webserver 1 EC2 instance
```
$ redis-cli
127.0.0.1:6379> del globalTopTen
(integer) 1
127.0.0.1:6379> del outageKey
(integer) 1
127.0.0.1:6379> del theftKey
(integer) 1
127.0.0.1:6379> exit
```
the integer values are boolean representations, 1 for true and 0 for false. If
you receive a 0 after issuing the del command, the table may not exist.
