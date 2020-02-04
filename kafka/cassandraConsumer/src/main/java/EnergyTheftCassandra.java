import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EnergyTheftCassandra {

    private static final String KEYSPACE = "geoTime";
    private static final String TABLE_NAME = "anomalyDetection";

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(CumulativeSumConsumer.class.getName());
        String bootstrapServers = "localhost:9092";
        String groupId = "theftCassandra";

        CommonConsumer commonConsumer = new CommonConsumer();
        Properties properties = commonConsumer.setKafkaProperties(groupId);

        // create consumer
        KafkaConsumer<String, Double> consumer =
                new KafkaConsumer<String, Double>(properties);

        Pattern regexP = Pattern.compile("([a-zA-Z0-9]+)(.*)");

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList("theft"));

        // poll for new data
        while (true) {
            // set language to 8
            ConsumerRecords<String, Double> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, Double> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                Matcher m = regexP.matcher(record.key().toString());
                if (m.find()) {
                    String geoHashKey = m.group(1);
                    System.out.println(geoHashKey);
                }

            }
        }


    }
}
