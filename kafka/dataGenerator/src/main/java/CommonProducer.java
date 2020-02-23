import fabricator.Location;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class CommonProducer {

    public KafkaProducer<String, String> createProducer() throws FileNotFoundException, IOException {
        // (0) set kafka variables
        String basePath = new File("").getAbsolutePath();
        String pathToProps = basePath+"/private.properties";

        Properties props2 = new Properties();
        FileInputStream fis = new FileInputStream(pathToProps);
        props2.load(fis);
        String bootstrapServer1 = props2.getProperty("bootstrapServer1");
        String bootstrapServer2 = props2.getProperty("bootstrapServer2");
        String bootstrapServer3 = props2.getProperty("bootstrapServer3");

        List<String> listOfServers = Arrays.asList(bootstrapServer1, bootstrapServer2, bootstrapServer3);

        String listOfStrServers = String.join(",", listOfServers);

        String bootstrapServer = bootstrapServer2;
        String batchSize = "40000";
        String linger = "10"; // the amount of milliseconds for kafka to wait before batching.
        String acks = "all";


        // (1) create Producer Properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize); // default batch size is 16384 bytes
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, linger); // default linger is 0 ms
        properties.setProperty(ProducerConfig.ACKS_CONFIG, acks);


        // (2) create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    public static double round(double value, int places) {
        // a method to round double values
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(Double.toString(value));
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();

    }

    public static ArrayList<Double> createCoordinateList(int start,
                                                         int stop,
                                                         double coordinate,
                                                         double diffRound) {
        // method to create a list of either latitude or longitude values
        ArrayList<Double> list = new ArrayList<Double>();
        for (int i = start; i < stop; i++) {
            if (i == start) list.add(coordinate);
            else {
                coordinate += diffRound;
                list.add(round(coordinate, 6));
            }
        }
        return list;
    }

    public static ArrayList<Double> generateIntervals() {

        // the diffRound value is used to increment the latitude value during each iteration of
        // the for loop
        double difference = (51.480612 - 51.439006) / 172;
        double diffRound = round(difference, 6);

        double longiDifference = -1 * (-0.036371 - -0.096624) / 172;
        double diffLongiRound = round(longiDifference, 6);

        ArrayList<Double> initialLongitudes = new ArrayList<Double>() {
            {
                add(diffRound);
                add(diffLongiRound);

            }
        };

        return initialLongitudes;


    }

    public static void produceToKafka(Location location, ArrayList<Double> listOfLatitudes,
                                      ArrayList<Double> listOfLongitudes, String kafkaTopic,
                                      KafkaProducer<String, String> producer,
                                      Logger logger,
                                      String thisProducer) {

        // schema: time stamp, geohash, energy

        //time stamp value (time the measurement was taken!)
        Instant now = Instant.now();
        Instant nowNoMilli = now.truncatedTo(ChronoUnit.SECONDS);

        String timestampstr = nowNoMilli.toString();

        String pattern = "yyyy-MM-dd'T'HH:mm:ssX";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        LocalDateTime localDateTime = LocalDateTime.from(formatter.parse(timestampstr));

        Timestamp tsNotString = Timestamp.valueOf(localDateTime);
        String tsString = tsNotString.toString();


        // Create ~3,333 geohashes:
        for (int i = 0; i < listOfLatitudes.size(); i++) {
            for (int j = 0; j < listOfLongitudes.size(); j++) {

                // kafka key per record
                String kafkaKey = "id_" + thisProducer + Integer.toString(i) + Integer.toString(j);
                // geohash value
                double latCoord = listOfLatitudes.get(i);
                double longCoord = listOfLongitudes.get(j);
                String geohash = location.geohash(latCoord, longCoord);

                // energy value
                Random r = new Random();
                double stdDev = 0.00015;
                double mean = 0.0005;
                double sampleEnergyVal = 0.0;
                sampleEnergyVal = round(r.nextGaussian() * stdDev + mean, 5);
                if (sampleEnergyVal < 0.0) {
                    sampleEnergyVal = 0.0;
                }
                String sampleEnergyValStr = String.format("%.5f", sampleEnergyVal);

                DataRecord dataRecord = new DataRecord(tsString, geohash, sampleEnergyValStr);
                JSONObject jo = new JSONObject(dataRecord);
                String jsonStr = jo.toString();

                ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, kafkaKey, jsonStr);

                // (3) send data
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // successfully sent
                            logger.info("Received new metadata: \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp() +
                                    "VALUES: " + record.value());
                        } else {
                            logger.error("Error while producing", e);

                        }
                    }
                });
            }
        }


    }
}
