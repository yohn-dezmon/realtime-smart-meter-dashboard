package kafkaStreams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.json.JSONObject;


import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
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


    public CommonStreams(String APPLICATION_ID, String INPUT_TOPIC,
                         String OUTPUT_TOPIC, String broker) {

        this.APPLICATION_ID = APPLICATION_ID;
        this.INPUT_TOPIC = INPUT_TOPIC;
        this.OUTPUT_TOPIC = OUTPUT_TOPIC;
        this.broker = broker;

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

    public void windowMovingAvg(KStream<String, Double> geohashEnergy,
                                int timeSeconds,
                                Double upperLimit,
                                Double lowerLimit) {
        geohashEnergy.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(timeSeconds)))
                .reduce((val1, val2) -> val1 + val2)
                .toStream()
                .map((key, value) -> {
                KeyValue<Windowed<String>, String> keyValue;
                boolean energyTheft = false;
                boolean outage = false;
                Timestamp timeStamp = getTimeStamp();

                Double windowSum = value;
                Double movingAvg = windowSum/timeSeconds;
                Double movingAvgRounded = round(movingAvg, 6);

                if (movingAvg > upperLimit) {
                    energyTheft = true;
                } else if (movingAvg <= lowerLimit) {
                    outage = true;
                }

                MovingAvgRecord movingAvgRecord = new MovingAvgRecord(timeStamp,
                        movingAvg,
                        energyTheft,
                        outage);

                String jsonStr = jsonToStr(movingAvgRecord);

                keyValue = new KeyValue<Windowed<String>, String>(key, jsonStr);
                System.out.println("KEY: "+key+" "+jsonStr);

                return keyValue;

        }).to(OUTPUT_TOPIC, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));
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
            streams.cleanUp();
            streams.start();

            latch.await();

        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public static Double round(Double value, int places) {
        // a method to round double values
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(Double.toString(value));
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();

    }

    public static Timestamp getTimeStamp() {
        //time stamp value (time the measurement was taken!)
        Date date = new Date();
        long time = date.getTime();
        Timestamp ts = new Timestamp(time);

        return ts;
    }

    public static String jsonToStr(MovingAvgRecord movingAvgRecord) {
        JSONObject jo = new JSONObject(movingAvgRecord);
        String jsonStr = jo.toString();
        return jsonStr;
    }



}