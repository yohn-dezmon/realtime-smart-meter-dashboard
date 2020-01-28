package kafka;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Properties;


public class AnomalyDetector {

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
        Logger logger = LoggerFactory.getLogger(AnomalyDetector.class);

        // initiate Kafka Streams Topology builder
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> preJson = cs.getRawValues(builder, INPUT_TOPIC);

        KStream<String, Double> geohashEnergy = cs.getGeoEnergy(preJson);

        KeyValueStore<String, ArrayList<Double>> listState = cs.listState;

        cs.getEmptyList(geohashEnergy);

        int timeWindow = 5; // represents 5 second time window

        cs.cacheLatestValues(geohashEnergy, listState, timeWindow);

        cs.runKafkaStreams(builder, props);


    }


}
