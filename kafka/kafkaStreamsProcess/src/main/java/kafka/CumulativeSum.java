package kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class CumulativeSum {
    // keeping track of cumulative sum of energy for each geohash, to be able to find
    // the user who is using the most energy out of all of the users

    static String APPLICATION_ID = "application";
    static String INPUT_TOPIC="fake_iot";
    static String OUTPUT_TOPIC="streams-pipe-output";
    static String broker = "127.0.0.1:9092";

    public static void main(String[] args) throws Exception, FileNotFoundException, IOException {
        String basePath = new File("").getAbsolutePath();
        String pathToProps = basePath + "/private.properties";


        Properties props2 = new Properties();
        FileInputStream fis = new FileInputStream(pathToProps);

        props2.load(fis);

        String ip1 = props2.getProperty("kafka1");
        String ip2 = props2.getProperty("kafka2");
        String ip3 = props2.getProperty("kafka3");

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> record = builder.stream(INPUT_TOPIC);
        Arrays.asList(value.toLowerCase().split("\\W+"))

        KStream postSplit = record.flatMapValues(value -> Arrays.asList(value.split(",")))
                .map((key, value) -> key.);


        KStream afterProcess = record
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(1)))
                .reduce((val1,val2)->val1+val2)
                .toStream();

        afterProcess.to(OUTPUT_TOPIC, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();


    }

}
