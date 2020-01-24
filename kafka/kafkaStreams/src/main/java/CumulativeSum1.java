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

public class CumulativeSum1 {
    // keeping track of cumulative sum of energy for each geohash, to be able to find
    // the user who is using the most energy out of all of the users

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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cumulative-sum");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" + ip2 + ":9092" + ip3 + ":9092");
        // Leaving key and values as strings for now, may need to change later for optimization
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> record = builder.stream("fake-iot");
        final CountDownLatch latch = new CountDownLatch(1);

//        System.out.println(record.groupBy(key, value));


//        KTable<String, Long> wordCounts = record
//                // Split each text line, by whitespace, into words.
//                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(",")))
//
//                // Group the text words as message keys
//                .groupBy((key, value) -> value)
//
//                // Count the occurrences of each word (message key).
//                .count();

// Store the running counts as a changelog stream to the output topic.
//        wordCounts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        // attach shutdown handler to catch control-c
//        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
//            @Override
//            public void run() {
//                streams.close();
//                latch.countDown();
//            }
//        });
//
//        try {
//            streams.start();
//            latch.await();
//        } catch (Throwable e) {
//            System.exit(1);
//        }
//        System.exit(0);
//    }


        KStream afterProcess = record
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(1)))
                .reduce((val1, val2) -> val1 + val2)
                .toStream();

        afterProcess.to("streams-pipe-output", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

    }

}