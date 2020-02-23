import fabricator.Fabricator;
import fabricator.Location;
import org.apache.kafka.clients.producer.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class ProducerSmartMeter {

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {


        // create a logger for this class
        Logger logger = LoggerFactory.getLogger(ProducerSmartMeter.class);
        String kafkaTopic = "fake_iot";

        CommonProducer commonProducer = new CommonProducer();
        KafkaProducer<String, String> producer = commonProducer.createProducer();

        // this is combined with the kafkaKey to make unique keys for this producer
        String thisProducer = "prod";

        // third party module to generate geohashes, used in produceToKafka() method
        Location location = Fabricator.location();


        double initialLatitude = 51.439006;
        double initialLongitude = -0.073757;

        ArrayList<Double> listOfIntervals = commonProducer.generateIntervals();
        double diffRound = listOfIntervals.get(0);
        double diffLongiRound = listOfIntervals.get(1);



        // create a list of latitudes and longitudes to later be converted to geohashes
        // 57 because 57*57 = ~3,333 and we want this producer to create 3,333 events/second
        ArrayList<Double> listOfLats = commonProducer.createCoordinateList(0, 57, initialLatitude, diffRound);
        ArrayList<Double> listOfLongs = commonProducer.createCoordinateList(0, 57,  initialLongitude, diffLongiRound);

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

        // a task that will be run continually by the executorService
        Runnable task1 = () -> {

        commonProducer.produceToKafka(location, listOfLats, listOfLongs, kafkaTopic, producer, logger, thisProducer);

        };

        // (3) send data to Kafka, this code executes every second
        executorService.scheduleAtFixedRate(task1, 0,2, TimeUnit.SECONDS);

        // set the time for the executor to run before terminating
        executorService.awaitTermination(120, TimeUnit.SECONDS);
        executorService.shutdown();

        producer.flush();
        producer.close();

    }


    }



