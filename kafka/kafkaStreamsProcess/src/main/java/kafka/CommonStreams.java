package kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;



public class CommonStreams {

    static String APPLICATION_ID;
    static String INPUT_TOPIC;
    static String OUTPUT_TOPIC;
    static String broker;

    // let's try just making this a HashMap?
    private HashMap<String, ArrayList<Double>> listState;
//    private ProcessorContext context;


    public CommonStreams(String APPLICATION_ID, String INPUT_TOPIC,
                         String OUTPUT_TOPIC, String broker) {

        this.APPLICATION_ID = APPLICATION_ID;
        this.INPUT_TOPIC = INPUT_TOPIC;
        this.OUTPUT_TOPIC = OUTPUT_TOPIC;
        this.broker = broker;
//        this.context = context;
        this.listState = new HashMap<String, ArrayList<Double>>();

    }

    public Properties setProperties() {

        // example key or value serde = Serdes.String().getClass()
        // Kafka configuration
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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
/*
    public KTable<String, Queue<Double>> getEmptyQueue(KStream<String, Double> geohashEnergy) {
        KStream<String, Queue<Double>> emptyQueueStream = geohashEnergy.map((key, value) -> {
            KeyValue<String, Queue<Double>> keyValue;

            Queue<Double> q = new LinkedList<Double>();

            keyValue = new KeyValue<>(key, q);
            return keyValue;
        });


        KTable<String, Queue<Double>> emptyQueueTable = emptyQueueStream.groupByKey().reduce(
                (key, value) -> value);

        return emptyQueueTable;

    }
    */


    public void getEmptyList(KStream<String, Double> geohashEnergy) {
        KStream<String, ArrayList<Double>> emptyListStream = geohashEnergy.map((key, value) -> {
            KeyValue<String, ArrayList<Double>> keyValue;

            ArrayList<Double> list = new ArrayList<Double>();
            System.out.println(key+" testinttesting");
            System.out.println(key.toString()+"testing2testing2");
            listState.put(key.toString(), list);
            keyValue = new KeyValue<>(key.toString(), list);
            return keyValue;
        });

    }

    public void cacheLatestValues(KStream<String, Double> geohashEnergy,
                                                           int timeWindow) {
        // starting key = geohash, starting value = energy (not yet grouped)
        // KTable<String, Double> finalTable
        geohashEnergy.map((key, value) -> {
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
        }).groupByKey().reduce(
                (key, value) -> value).toStream().to(OUTPUT_TOPIC);


    }

    public void runKafkaStreams(StreamsBuilder builder, Properties props) {
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);


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
