package kafkaStreams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;


import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * A class that contains functions that are common to all of
 * the Kafka Streams applications within this package. Some functions
 * are specific to a given Streams application, but have been written
 * here to make the Streams applications more readable.
 */

public class CommonStreams {


    static String APPLICATION_ID;
    static String INPUT_TOPIC;
    static String OUTPUT_TOPIC;
    static String broker;

    private static HashMap<String, ArrayList<Double>> listState;
    private KeyValueStore<String, ArrayList<Double>> keyValueStore;



    public CommonStreams(String APPLICATION_ID, String INPUT_TOPIC,
                         String OUTPUT_TOPIC, String broker) {

        this.APPLICATION_ID = APPLICATION_ID;
        this.INPUT_TOPIC = INPUT_TOPIC;
        this.OUTPUT_TOPIC = OUTPUT_TOPIC;
        this.broker = broker;
        this.listState = new HashMap<String, ArrayList<Double>>();

    }



    public Properties setProperties() {


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

    public void windowMovingAvg(KStream<String, Double> geohashEnergy, int timeSeconds) {
        geohashEnergy.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(3)))
                .reduce((val1, val2) -> val1 + val2)
                .toStream()
                .map((key, value) -> {
                KeyValue<Windowed<String>, Double> keyValue;

                Double windowSum = value;
                Double movingAvg = windowSum/timeSeconds;

                keyValue = new KeyValue<Windowed<String>, Double>(key, movingAvg);
                System.out.println("does the code ever enter this else statement?");
                System.out.println(key+": "+movingAvg.toString());

                return keyValue;

        }).to(OUTPUT_TOPIC, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.Double()));
    }

    /*

    public void getEmptyList(KStream<String, Double> geohashEnergy) {
        // ok... I want to put the geohash/ArrayList<Double> into a KeyValueStore

        Double valy = 0.0;
        ArrayListSerde<Double> arrayListSerde = new ArrayListSerde<Double>(Serdes.Double());

        KStream<String, ArrayList<Double>> emptyListStream = geohashEnergy.map((key, value) -> {
            KeyValue<String, ArrayList<Double>> keyValue;

            ArrayList<Double> list = new ArrayList<Double>();
            System.out.println(key+"     "+value);
            keyValueStore.put(key, list);
            listState.put(key, list);
            keyValue = new KeyValue<>(key, list);
            return keyValue;
            //Materialized<String, ArrayList<Double>, KeyValueStore<String, ArrayList<Double>>>as("state")
        }).groupByKey(Grouped.with(Serdes.String(),
                new ArrayListSerde<>(Serdes.Double())))
                .reduce((key, value) -> value)
                .toStream();


    }

     */

    /*
    public void cacheLatestValues(KStream<String, Double> geohashEnergy,
                                  int timeWindow) {
        // ...
        // starting key = geohash, starting value = energy (not yet grouped)
        KStream<String, Double> movingAvgs = geohashEnergy.map((key, value) -> {
            KeyValue<String, Double> keyValue;

            // currently this array list
//            ArrayList<Double> list = listState.get(key);
            ArrayList<Double> list = keyValueStore.get(key);
            if (list.isEmpty() || list.size() < timeWindow) {
                list.add(value);
                System.out.println(key+": "+list.toString());
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
                System.out.println("does the code ever enter this else statement?");
                System.out.println(key+": "+movingAvg.toString());

                return keyValue;
            }
            keyValue = new KeyValue<>(key, 0.0);
            return keyValue;
        });


        movingAvgs.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Double()));

    }
*/
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
            streams.cleanUp();
            streams.start();

            latch.await();

        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }



}