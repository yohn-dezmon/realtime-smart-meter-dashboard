package kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;



public class CommonStreams {

    static String APPLICATION_ID;
    static String INPUT_TOPIC;
    static String OUTPUT_TOPIC;
    static String broker;

    private HashMap<String, ArrayList<Double>> listState;



    public CommonStreams(String APPLICATION_ID, String INPUT_TOPIC,
                         String OUTPUT_TOPIC, String broker) {

        this.APPLICATION_ID = APPLICATION_ID;
        this.INPUT_TOPIC = INPUT_TOPIC;
        this.OUTPUT_TOPIC = OUTPUT_TOPIC;
        this.broker = broker;
        this.listState = new HashMap<String, ArrayList<Double>>();

    }

    public Properties setProperties() {

        // example key or value serde = Serdes.String().getClass()
        // Kafka configuration
        Properties props = new Properties();
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());



        return props;
    }

    public KStream<String, String> getRawValues(StreamsBuilder builder, String INPUT_TOPIC) {
        // Get the data from kafka topic as a key/value pair of strings
        KStream<String, String> preJson = builder.stream(INPUT_TOPIC);

        return preJson;
    }

    public KStream<String, Double> getGeoEnergy(KStream<String, String> preJson) {


        // map the topic key/value pair to a new key/value pair where key = geohash and value = energy
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

        return geohashEnergy;

    }


    public void getEmptyList(KStream<String, Double> geohashEnergy) {
        KStream<String, ArrayList<Double>> emptyListStream = geohashEnergy.map((key, value) -> {
            KeyValue<String, ArrayList<Double>> keyValue;

            ArrayList<Double> list = new ArrayList<Double>();
            System.out.println(key+"     "+value);
            listState.put(key, list);
            keyValue = new KeyValue<>(key, list);
            return keyValue;
        });

    }

    public void cacheLatestValues(KStream<String, Double> geohashEnergy,
                                                           int timeWindow) {
        // starting key = geohash, starting value = energy (not yet grouped)
        // KTable<String, Double> finalTable
        KStream<String, Double> movingAvgs = geohashEnergy.map((key, value) -> {
            KeyValue<String, Double> keyValue;

            ArrayList<Double> list = listState.get(key);
            if (list.isEmpty() || list.size() < timeWindow) {
                list.add(value);
            } else {
                Double cumulativeSum = 0.0;
                for (int i = 0; i < list.size(); i++) {
                    cumulativeSum += list.get(i);
                }
                cumulativeSum += value;
                Double movingAvg = cumulativeSum/timeWindow;
                list.remove(0);
                list.add(value);
                keyValue = new KeyValue<>(key, movingAvg);

                return keyValue;
            }
            keyValue = new KeyValue<>(key, 0.0);
            return keyValue;
        });

//        Produced.with(Serdes.String(), Serdes.Double()

        movingAvgs.to(OUTPUT_TOPIC);

    }

    public void runKafkaStreams(StreamsBuilder builder, Properties props) {
        System.out.println("is this where?");
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
        System.out.println("Count Down Latch...");

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Runtime hook...");
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.cleanUp();
            streams.start();
            System.out.println("Streams started...");
            latch.await();
            System.out.println("latch awaiting");
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }



}
