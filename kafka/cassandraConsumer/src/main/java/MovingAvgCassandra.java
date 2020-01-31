import com.datastax.driver.core.Session;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovingAvgCassandra {

    private static final String KEYSPACE = "geoTime";
    private static final String TABLE_NAME = "movingAvg";

    public static void main(String[] args) {

        CommonCassandra cc = new CommonCassandra(KEYSPACE);

        cc.connect("10.0.0.5", 9042);

        Session session = cc.getSession();

        cc.createKeySpace(KEYSPACE, "SimpleStrategy",
                1);
        cc.useKeyspace(KEYSPACE);
//        cc.createCumulativeSumTable();


        // if you miss the tab for the class, you can get back to that
        // drop down menu with alt+Tab
        Logger logger = LoggerFactory.getLogger(CumulativeSumConsumer.class.getName());
        String bootstrapServers = "localhost:9092";
        String groupId = "anomalyDetectorCassandra";

        // New consumer configs (Kafka docs)
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // "earliest/latest/none"

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

                Matcher m = regexP.matcher(record.key().toString());
                if (m.find()) {
                    String geoHashKey = m.group(1);
                    System.out.println(geoHashKey);
                }
//                cc.insertToCumulativeSumTable(record.key(), record.value());

            }
        }


    }
}
