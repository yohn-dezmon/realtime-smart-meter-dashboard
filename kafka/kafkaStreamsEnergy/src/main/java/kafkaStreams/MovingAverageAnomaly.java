package kafkaStreams;


import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MovingAverageAnomaly {


    static String APPLICATION_ID = "anomaly-detector";
    static String INPUT_TOPIC="fake_iot";
    static String OUTPUT_TOPIC="anomaly";
    static String broker = "127.0.0.1:9092";


    int timeWindow = 5; // 5 second time window

    public static void main(String[] args) {

        CommonStreams cs = new CommonStreams(APPLICATION_ID, INPUT_TOPIC,
                OUTPUT_TOPIC, broker);
        Properties props = cs.setProperties();


        // create a logger for this class
        Logger logger = LoggerFactory.getLogger(MovingAverageAnomaly.class);

        // initiate Kafka Streams Topology builder
        final StreamsBuilder builder = new StreamsBuilder();


        KStream<String, String> preJson = cs.getRawValues(builder, INPUT_TOPIC);
        KStream<String, Double> geohashEnergy = cs.getGeoEnergy(preJson);

        int timeWindow = 5; // represents 5 second time window
        Double upperLimit = 0.008; // represents two standard deviations above mean
        Double lowerLimit = 0.0; // represents lower limit indicating outage
        cs.windowMovingAvg(geohashEnergy, timeWindow, upperLimit, lowerLimit);


        cs.runKafkaStreams(builder, props);


    }

}