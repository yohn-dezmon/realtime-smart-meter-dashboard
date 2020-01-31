import com.datastax.driver.core.Session;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class CumulativeSumConsumer {

    private static final String KEYSPACE = "geoTime";
    private static final String TABLE_NAME = "cumulativeSum";
    private static final String KEYSPACE_TABLE = KEYSPACE+"."+TABLE_NAME;


    public static void main(String[] args) {

        CommonCassandra cc = new CommonCassandra(KEYSPACE);

        cc.connect("10.0.0.5", 9042);

        Session session = cc.getSession();

        cc.createKeySpace(KEYSPACE, "SimpleStrategy",
                1);
        cc.useKeyspace(KEYSPACE);
        cc.createCumulativeSumTable(TABLE_NAME);

        Logger logger = LoggerFactory.getLogger(CumulativeSumConsumer.class.getName());
        String bootstrapServers = "localhost:9092";
        String groupId = "sumToCassandra";

        CommonConsumer commonConsumer = new CommonConsumer();


        Properties properties = commonConsumer.setKafkaProperties(groupId);

        // create consumer
        KafkaConsumer<String, Double> consumer =
                new KafkaConsumer<String, Double>(properties);

        // subscribe consumer to our topic(s)

        consumer.subscribe(Arrays.asList("cumulativesum"));

        // poll for new data
        while (true) {
            // set language to 8
            ConsumerRecords<String, Double> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, Double> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                cc.insertToCumulativeSumTable(record.key(), record.value(), KEYSPACE_TABLE);

            }
        }

    }


}
