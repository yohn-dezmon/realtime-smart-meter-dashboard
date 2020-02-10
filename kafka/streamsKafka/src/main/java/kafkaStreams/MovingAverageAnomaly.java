package kafkaStreams;


import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.regex.Pattern;

public class MovingAverageAnomaly {


    static String APPLICATION_ID = "anomaly-detector";
    static String INPUT_TOPIC="fake_iot";
    static String OUTPUT_MOVAVG="movingavg";
    static String OUTPUT_THEFT="theft";
    static String OUTPUT_OUTAGE="outage";
    static String broker = "127.0.0.1:9092";


    int timeWindow = 5; // 5 second time window

    public static void main(String[] args) {

        CommonStreams cs = new CommonStreams(APPLICATION_ID,
                                            INPUT_TOPIC,
                                            broker);
        Properties props = cs.setProperties();

        // create a logger for this class
        Logger logger = LoggerFactory.getLogger(MovingAverageAnomaly.class);

        // initiate Kafka Streams Topology builder
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> preJson = cs.getRawValues(builder, INPUT_TOPIC);
        KStream<String, Double> geohashEnergy = cs.getGeoEnergy(preJson);

        int timeWindow = 5; // represents 5 second time window
        Double upperLimit = 0.0007; // represents ~ two standard deviations above mean
        Double lowerLimit = 0.0000; // lower limit for notification system

        cs.windowMovingAvg(geohashEnergy,
                            timeWindow,
                            upperLimit,
                            lowerLimit,
                            OUTPUT_MOVAVG,
                            OUTPUT_THEFT,
                            OUTPUT_OUTAGE);

        cs.runKafkaStreams(builder, props);

    }

}