import com.datastax.driver.core.Session;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovingAvgCassandra {

    private static final String KEYSPACE = "geoTime";
    private static final String TABLE_NAME = "movingavg";
    private static final String KEYSPACE_TABLE = KEYSPACE+"."+TABLE_NAME;

    public static void main(String[] args) throws FileNotFoundException, IOException {

        CommonCassandra cc = new CommonCassandra(KEYSPACE);
        CommonConsumer commonConsumer = new CommonConsumer();

        String basePath = new File("").getAbsolutePath();
        String pathToProps = basePath+"/private.properties";

        Properties props2 = new Properties();
        FileInputStream fis = new FileInputStream(pathToProps);
        props2.load(fis);
        String ip1 = props2.getProperty("cassandra1");
        cc.connect(ip1, 9042);

        Session session = cc.getSession();

        cc.createKeySpace(KEYSPACE, "SimpleStrategy",
                1);
        cc.useKeyspace(KEYSPACE);
        cc.createMovingAvgTable(TABLE_NAME);

        // if you miss the tab for the class, you can get back to that
        // drop down menu with alt+Tab
        Logger logger = LoggerFactory.getLogger(MovingAvgCassandra.class.getName());
        String bootstrapServers = "localhost:9092";
        String groupId = "anomalyDetectorCassandra";

        Properties properties = commonConsumer.setKafkaProperties(groupId);

        // create consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);

        Pattern regexP = Pattern.compile("([a-zA-Z0-9]+)(.*)");

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList("movingavg"));

        // poll for new data
        while (true) {
            // set language to 8
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value().toString());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                String geohashKey = "a";
                Matcher m = regexP.matcher(record.key().toString());

                if (m.find()) {
                    geohashKey = m.group(1);
                    System.out.println(geohashKey);
                }
                // Value:0.00061,false,false,2020-02-04 14:15:34.918,
                String[] arr = record.value().split(",");
                String movingavg = arr[0];
                String timestamp = arr[3];

             cc.insertToMovingAvgTable(geohashKey, timestamp, movingavg, KEYSPACE_TABLE);

            }
        }


    }
}
