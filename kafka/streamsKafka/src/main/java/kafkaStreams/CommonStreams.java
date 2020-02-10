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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/**
 * A class that contains functions that are common to all of
 * the Kafka Streams applications within this package. Some functions
 * are specific to a given Streams application, but have been written
 * here to make the Streams applications more readable.
 */

public class CommonStreams {


    private String APPLICATION_ID;
    private String INPUT_TOPIC;
    // because there are variable numbers of output topics, I will not include them as
    // instance variables
    static String broker;


    public CommonStreams(String APPLICATION_ID,
                         String INPUT_TOPIC,
                         String broker
                         ) {

        this.APPLICATION_ID = APPLICATION_ID;
        this.INPUT_TOPIC = INPUT_TOPIC;
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
                                Double lowerLimit,
                                String OUTPUT_MOVAVG,
                                String OUTPUT_THEFT,
                                String OUTPUT_OUTAGE)
    {

         KStream<String, ArrayList<String>> preBranching = geohashEnergy.groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(timeSeconds)))
                .reduce((val1, val2) -> val1 + val2)
                .toStream()
                .map((key, value) -> {
                KeyValue<String, ArrayList<String>> keyValue;
                boolean energyTheft = false;
                boolean outage = false;
                String timeStamp = getTimeStamp();

                Double windowSum = value;
                Double movingAvg = windowSum/timeSeconds;
                Double movingAvgRounded = round(movingAvg, 6);
                String movingAvgStr = String.format("%.5f", movingAvgRounded);

                if (movingAvg >= upperLimit) {
                    energyTheft = true;
                } else if (movingAvg <= lowerLimit) {
                    outage = true;
                }

                String keyStr = key.toString();
                MovingAvgRecord movingAvgRecord = new MovingAvgRecord(timeStamp,
                        movingAvgStr,
                        energyTheft,
                        outage);

                String jsonStr = jsonToStr(movingAvgRecord);
                ArrayListSerde<String> arrayListSerdeSerde =
                        new ArrayListSerde<String>(Serdes.String());

                ArrayList<String> list = new ArrayList();

                    list.add(movingAvgStr);
                    list.add(String.valueOf(energyTheft));
                    list.add(String.valueOf(outage));
                    list.add(timeStamp.toString());

                keyValue = new KeyValue<String, ArrayList<String>>(keyStr, list);
                System.out.println("KEY: "+keyStr+" "+list.toString());

                return keyValue;

        });

         // all data going to OUTPUT_MOVAVG despite it being an anomaly or not
         preBranching.to(OUTPUT_MOVAVG, Produced.with(Serdes.String(), new ArrayListSerde<String>(Serdes.String())));

        KStream<String, ArrayList<String>>[] branches = preBranching.branch(
                         (key, value) -> value.get(1) == "true",
                         (key, value) -> value.get(2) == "true"
                 );

        // send key value pairs to OUTPUT_THEFT because their moving averages are above the upper limit
        branches[0].to(OUTPUT_THEFT, Produced.with(Serdes.String(), new ArrayListSerde<String>(Serdes.String())));

        // send key value pairs to OUTPUT_OUTAGE because moving average below lower limit
        branches[1].to(OUTPUT_OUTAGE, Produced.with(Serdes.String(), new ArrayListSerde<String>(Serdes.String())));

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

    public static String getTimeStamp() {
        //time stamp value (time the measurement was taken!)
        Instant now = Instant.now();
        Instant nowNoMilli = now.truncatedTo(ChronoUnit.SECONDS);
        String timestampstr = nowNoMilli.toString();

        String pattern = "yyyy-MM-dd'T'HH:mm:ssX";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(timestampstr));

        Timestamp tsNotString = Timestamp.valueOf(localDateTime);
        String ts = tsNotString.toString();

        return ts;
    }

    public static String jsonToStr(MovingAvgRecord movingAvgRecord) {

        JSONObject jo = new JSONObject(movingAvgRecord);
        String jsonStr = jo.toString();
        return jsonStr;
    }



}