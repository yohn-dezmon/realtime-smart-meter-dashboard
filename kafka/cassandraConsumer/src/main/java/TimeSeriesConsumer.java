import com.datastax.driver.core.Session;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
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


public class TimeSeriesConsumer {

    private static final String KEYSPACE = "geoTime";
    private static final String TIMESERIES_TABLE = "simpleTimeSeries";
    private static final String INDIV_TIMESERIES_TABLE = "indivTimeSeries";
    private static final String TIMESERIES_KEYSPACE = KEYSPACE+"."+TIMESERIES_TABLE;
    private static final String INDIV_KEYSPACE = KEYSPACE+"."+INDIV_TIMESERIES_TABLE;

    public static void main(String[] args) {
        CommonCassandra cc = new CommonCassandra(KEYSPACE);

            // connect to cassandra
            cc.connect("10.0.0.5", 9042);

            Session session = cc.getSession();

            cc.createKeySpace(KEYSPACE, "SimpleStrategy",
                    1);
            cc.useKeyspace(KEYSPACE);
            cc.createIndividualTimeSeriesTable(INDIV_TIMESERIES_TABLE);
            cc.createTimeSeriesTable(TIMESERIES_TABLE);

            //this.KEYSPACE_TABLE = KEYSPACE+"."+TABLE_NAME;


            // if you miss the tab for the class, you can get back to that
            // drop down menu with alt+Tab
            Logger logger = LoggerFactory.getLogger(TimeSeriesConsumer.class.getName());
            String bootstrapServers = "localhost:9092";
            String groupId = "timeseriesToCassandra";

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

            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList("fake_iot"));

            // poll for new data
            while (true) {
                // set language to 8
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                    String jsonStr = record.value();

                    try {
                        JsonObject jsonObject = new JsonParser().parse(jsonStr).getAsJsonObject();
                        String timestamp = jsonObject.get("date").getAsString();
                        String geohash = jsonObject.get("geohash").getAsString();
                        String energyVal = jsonObject.get("energyVal").getAsString();

                        cc.insertToTimeSeriesTable(geohash, timestamp, energyVal, TIMESERIES_KEYSPACE);
                        cc.insertToIndividualTimeSeriesTable(geohash, timestamp, energyVal, INDIV_KEYSPACE);

                    } catch (JsonSyntaxException e) {
                        e.printStackTrace();
                    }


                }
            }


    }

}
