import fabricator.Fabricator;
import fabricator.Location;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Producer2SmartMeter {

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {

        // create a logger for this class
        Logger logger = LoggerFactory.getLogger(Producer2SmartMeter.class);
        String kafkaTopic = "fake_iot";

        CommonProducer commonProducer = new CommonProducer();
        KafkaProducer<String, String> producer = commonProducer.createProducer();
        String thisProducer2 = "prod2";

        // third party module to generate geohashes, used in produceToKafka() method
        Location location = Fabricator.location();

        double initialLatitude = 51.452318;
        double initialLongitude = -0.055667;

        ArrayList<Double> listOfIntervals = commonProducer.generateIntervals();
        double diffRound = listOfIntervals.get(0);
        double diffLongiRound = listOfIntervals.get(1);

        ArrayList<Double> listOfLats1 = commonProducer.createCoordinateList(57, 114, initialLatitude, diffRound);
        ArrayList<Double> listOfLongs1 = commonProducer.createCoordinateList(57, 114,  initialLongitude, diffLongiRound);

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

        // a task that will be run continually by the executorService
        Runnable task1 = () -> {
        commonProducer.produceToKafka(location, listOfLats1, listOfLongs1, kafkaTopic, producer, logger, thisProducer2);

        };

        // (3) send data to Kafka, this code executes every 5 seconds
        executorService.scheduleAtFixedRate(task1, 0,5, TimeUnit.SECONDS);

        // set the time for the executor to run before terminating
        executorService.awaitTermination(120, TimeUnit.SECONDS);
        executorService.shutdown();
        producer.flush();
        producer.close();

    }
}
