package kafka;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class JsonConnectSum {

    static String APPLICATION_ID = "application";
    static String INPUT_TOPIC="fake_iot";
    static String OUTPUT_TOPIC="streams-pipe-output";
    static String broker = "127.0.0.1:9092";


    public static void main(String[] args) {
        /*
        Kafka Streams application using the org.apache.kafka connect-json maven package
        to deserialize JSON from kafka topic...
        Following the model found here: https://github.com/amient/hello-kafka-streams/blob/master/src/main/java/io/amient/examples/wikipedia/WikipediaStreamDemo.java
         */
                final Serializer<JsonNode> jsonSerializer = new JsonSerializer();


        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);


        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a logger for this class
        Logger logger = LoggerFactory.getLogger(JsonConnectSum.class);


        final StreamsBuilder builder = new StreamsBuilder();


        KStream<String, String> preJson = builder.stream(INPUT_TOPIC);

        System.out.println("Made it thus far");

        // KStream<String, JsonNode> convToJson =
        KStream<String, Double> geohashEnergy = preJson.map((key, value) -> {
             KeyValue<String, Double> keyValue;


            String jsonStr = value;
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode jsonValues = mapper.readTree(jsonStr);

                JsonNode geohash = jsonValues.get("geohash");
                JsonNode energy = jsonValues.get("energyVal");
                System.out.println(geohash+" "+energy);

                String newKey = geohash.asText();
                Double newValue = energy.asDouble();

                keyValue = new KeyValue<>(newKey, newValue);
                return keyValue;

            } catch (IOException e) {
                e.printStackTrace();
            }

            keyValue = new KeyValue<String, Double>("", 0.0);

            return keyValue;
        });

        System.out.println("Do I get this far?");

        geohashEnergy.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                 // Apply SUM aggregation
                 .reduce(Double::sum)
                 // Write to stream specified by outputTopic
                 .toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Double()));


        final Topology topology = builder.build();
//        System.out.println(topology.describe());
//        topology.addSource(INPUT_TOPIC, INPUT_TOPIC);
//        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
//        streams.cleanUp();

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
