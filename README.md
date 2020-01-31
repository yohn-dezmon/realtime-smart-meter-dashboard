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

The project consists of two clusters, one for Kafka and one for Cassandra.  
Zookeeper and Kafka and Java 8 must be installed on the instances on the Kafka Cluster. Once you have installed these technologies, you can clone this github repository to your EC2 instance.  
Before using the producers to pass in the data to Kafka, you should first create a topic called fake_iot.
The topic should consist of 6 partitions and a replication factor of at least two.    
To run the three Kafka producers in parallel, use the bashScripts/threeJavaProd.sh bash script.



## Kafka Details:

I created three Kafka producers, each of which generate a third of the simulated data. Each Kafka producer corresponds to a certain range of GPS coordinates, which are then converted into geohashes for their efficiency in storage (and also for graphing purposes). Each Kafka producer also produces a timestamp for a given set of values, and assigns an energy value to each row based on a normal distribution. Ultimately these data are supposed to represent 1 second interval readings from 10,000 homes in London, UK.
