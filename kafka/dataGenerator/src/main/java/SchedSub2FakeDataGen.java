import fabricator.Fabricator;
import fabricator.Location;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SchedSub2FakeDataGen {


    public static void main(String[] args) throws InterruptedException {
                long startTime = System.nanoTime();


        // create a logger for this class
        Logger logger = LoggerFactory.getLogger(SchedSub2FakeDataGen.class);

        String thisProducer = "prod2";

        Location location = Fabricator.location();

        double lattCoord = 51.452318;
        double longCoord = -0.055667;

        double difference = (51.480612 - 51.439006)/172;
        double diffRound = round(difference, 6);

        double longiDifference = -1*(-0.036371 - -0.096624)/172;
        double diffLongiRound = round(longiDifference, 6);


        // start = 0, stop =
        ArrayList<Double> listOfLatts = createCoordinateList(57, 114, lattCoord, diffRound);
        ArrayList<Double> listOfLongs = createCoordinateList(57, 114,  longCoord, diffLongiRound);

        // (0) set kafka variables
        String bootstrapServers = "127.0.0.1:9092";
        String kafkaTopic = "fake_iot";
        String batchSize = "40000";
        String linger = "10";
        String acks = "0";


        // (1) create Producer Properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // kafka will convert whatever we send to bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // default batch size is 16384 bytes
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        // default linger is 0
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, linger);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, acks);
        // (2) create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        Runnable task2 = () -> {
            produceToKafka(location, listOfLatts, listOfLongs, kafkaTopic, producer, logger, thisProducer);

        };

        // (3) send data to Kafka
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(task2, 0,1,TimeUnit.SECONDS);

        executorService.awaitTermination(10, TimeUnit.SECONDS);
        executorService.shutdown();

        producer.flush();
        producer.close();

        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        // with producer, this took 7.855 seconds
        System.out.println("Execution time in milliseconds : " + timeElapsed/1000000);

    }


    private static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(Double.toString(value));
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();

    }

    private static void produceToKafka(Location location, ArrayList<Double> listOfLattitudes,
                                       ArrayList<Double> listOfLongitudes, String kafkaTopic,
                                       KafkaProducer<String, String> producer,
                                       Logger logger,
                                       String thisProducer) {
        // schema: time stamp, geohash, energy

        //time stamp value (time the measurement was taken!)
        Date  date = new Date();
        long time = date.getTime();
        Timestamp ts = new Timestamp(time);
        String tsString = ts.toString();



        // Create ~3,333 geohashes:
        for (int i = 0; i < listOfLattitudes.size(); i++) {
            for (int j = 0; j < listOfLongitudes.size(); j++) {

                // kafka key per record
                String kafkaKey = "id_" + thisProducer + Integer.toString(i) + Integer.toString(j);
                // geohash value
                double latCoord = listOfLattitudes.get(i);
                double longCoord = listOfLongitudes.get(j);
                String geohash = location.geohash(latCoord, longCoord);


                // energy value
                Random  r = new Random();
                double stdDev = 0.00015;
                double mean = 0.0005;
                double sampleEnergyVal = round(r.nextGaussian()*stdDev+mean, 5);
                String sampleEnergyValStr = String.format("%.5f", sampleEnergyVal);


                String row = String.join(", ", tsString, geohash, sampleEnergyValStr);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, kafkaKey, row);


//                System.out.println(row);
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
                                    "Timestamp: " + recordMetadata.timestamp());
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
