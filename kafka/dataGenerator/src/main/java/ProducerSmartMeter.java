import fabricator.Fabricator;
import fabricator.Location;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProducerSmartMeter {

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {


        // create a logger for this class
        Logger logger = LoggerFactory.getLogger(ProducerSmartMeter.class);

        // (0) set kafka variables
        // reference all three servers in case one broker goes down
        String bootstrapServers = "127.0.0.1:9092";
        String kafkaTopic = "fake_iot";
        String batchSize = "40000";
        String linger = "10"; // the amount of milliseconds for kafka to wait before batching.
        String acks = "all";
        String timeout = "40000";

        // (1) create Producer Properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize); // default batch size is 16384 bytes
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, linger); // default linger is 0 ms
        properties.setProperty(ProducerConfig.ACKS_CONFIG, acks);
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout);

        // (2) create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // this is combined with the kafkaKey to make unique keys for this producer
        String thisProducer = "prod";
        String thisProducer1 = "prod1";
        String thisProducer2 = "prod2";

        // third party module to generate geohashes, used in produceToKafka() method
        Location location = Fabricator.location();

        // the initial latitude and longitude coordinates from which the createCoordinateList() will pull
        ArrayList<Double> initialLatitudes = new ArrayList<Double>() {
            {
                add(51.439006);
                add(51.452318);
                add(51.464798);
            }
        };

        ArrayList<Double> initialLongitudes = new ArrayList<Double>() {
            {
                add(-0.036371);
                add(-0.055667);
                add(-0.073757);
            }
        };

        // the diffRound value is used to increment the latitude value during each iteration of
        // the for loop
        double difference = (51.480612 - 51.439006)/172;
        double diffRound = round(difference, 6);

        double longiDifference = -1*(-0.036371 - -0.096624)/172;
        double diffLongiRound = round(longiDifference, 6);

        // create a list of latitudes and longitudes to later be converted to geohashes
        // 57 because 57*57 = ~3,333 and we want this producer to create 3,333 events/second
        ArrayList<Double> listOfLats = createCoordinateList(0, 57, initialLatitudes.get(0), diffRound);
        ArrayList<Double> listOfLongs = createCoordinateList(0, 57,  initialLongitudes.get(0), diffLongiRound);

        ArrayList<Double> listOfLats1 = createCoordinateList(57, 114, initialLatitudes.get(1), diffRound);
        ArrayList<Double> listOfLongs1 = createCoordinateList(57, 114,  initialLongitudes.get(1), diffLongiRound);

        ArrayList<Double> listOfLats2 = createCoordinateList(114, 172, initialLatitudes.get(2), diffRound);
        ArrayList<Double> listOfLongs2 = createCoordinateList(114, 172,  initialLatitudes.get(2), diffLongiRound);


        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

        // a task that will be run continually by the executorService
        Runnable task1 = () -> {
            produceToKafka(location, listOfLats, listOfLongs, kafkaTopic, producer, logger, thisProducer);

        };
        Runnable task2 = () -> {
            produceToKafka(location, listOfLats1, listOfLongs1, kafkaTopic, producer, logger, thisProducer1);
        };

        Runnable task3 = () -> {
            produceToKafka(location, listOfLats2, listOfLongs2, kafkaTopic, producer, logger, thisProducer2);
        };


        // (3) send data to Kafka, this code executes every second
        executorService.scheduleAtFixedRate(task1, 0,1, TimeUnit.SECONDS);
        executorService.scheduleAtFixedRate(task2, 0,1, TimeUnit.SECONDS);
        executorService.scheduleAtFixedRate(task3, 0,1, TimeUnit.SECONDS);

        // for now, the executorService will terminate after 20 seconds
        executorService.awaitTermination(120, TimeUnit.SECONDS);
        executorService.shutdown();

        producer.flush();
        producer.close();


    }


    private static double round(double value, int places) {
        // a method to round double values
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(Double.toString(value));
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();

    }


    private static void produceToKafka(Location location, ArrayList<Double> listOfLatitudes,
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
                                    "Partition: "+ recordMetadata.partition() + "\n" +
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

    private static ArrayList<Double> createCoordinateList(int start,
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
}
