package kafkaStreams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * A Kafka Streams application to calculate the cumulative sum of energy
 * of each geohash (location) that is providing data to the pipeline.
 */

public class JsonConnectSum {

    static String APPLICATION_ID = "application";
    static String INPUT_TOPIC="fake_iot";
    static String OUTPUT_TOPIC="cumulativesum";


    public static void main(String[] args) throws FileNotFoundException, IOException {


        CommonStreams cs = new CommonStreams(APPLICATION_ID,
                                            INPUT_TOPIC);

        Properties props = cs.setProperties();

        // create a logger for this class
        Logger logger = LoggerFactory.getLogger(JsonConnectSum.class);
        // initiate Kafka Streams Topology builder
        final StreamsBuilder builder = new StreamsBuilder();

        // Get the data from kafka topic as a key/value pair of strings
        KStream<String, String> preJson = builder.stream(INPUT_TOPIC);
        KStream<String, Double> geohashEnergy = cs.getGeoEnergy(preJson);

        // group by the geohash, and sum all energy values for that geohash, output to new kafka topic
        geohashEnergy.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                // Apply SUM aggregation
                .reduce(Double::sum)
                // Write to stream specified by outputTopic
                .toStream().to(OUTPUT_TOPIC);

        cs.runKafkaStreams(builder, props);

    }

}
