import com.datastax.driver.core.Metadata;
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
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;


public class TimeSeriesConsumer {



    private Cluster cluster;
    private Session session;
    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesConsumer.class);


    // creating table
    private static final String TABLE_NAME = "simpleTimeSeries";

    // connect to Cassandra
    public void connect(String node, Integer port) {

        Builder b = Cluster.builder().addContactPoint(node);

        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();

        Metadata metadata = cluster.getMetadata();
        LOG.info("Cluster name: " + metadata.getClusterName());

        session = cluster.connect();

    }

    // get cassandra session
    public Session getSession() {
        return this.session;
    }

    // close cassandra connection
    public void close() {
        session.close();
        cluster.close();
    }

    // Creating Cassandra KeySpace
    public void createKeySpace(
            String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS")
                .append(keyspaceName).append("WITH replication = {")
                .append("'class':'").append(replicationStrategy)
                .append("','replication_factor':").append(replicationFactor)
                .append("};");

    }

    public void useKeyspace(String keyspace) {
        session.execute("USE " + keyspace);
    }

    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(TABLE_NAME).append("(")
                .append("geohash uuid, timestampcol timestamp, energy double, PRIMARY KEY(geohash, timestampcol)) ")
                .append("WITH CLUSTERING ORDER BY (timestampcol ASC);");

        final String query = sb.toString();
        session.execute(query);
    }

    public static void main(String[] args) {
        TimeSeriesConsumer tsc = new TimeSeriesConsumer();
        // connect to cassandra
        // I think I need to change the node to the public IP of
        // public ip = ec2-3-231-170-82.compute-1.amazonaws.com
        tsc.connect("10.0.0.5", 9042);

        Session session = tsc.getSession();

        tsc.createKeySpace("geoTime", "SimpleStrategy",
                1);
        tsc.useKeyspace("geoTime");



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
//        consumer.subscribe(Collections.singleton("first_topic"));
        consumer.subscribe(Arrays.asList("fake_iot"));

        // poll for new data
        while (true) {
            // set language to 8
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
            // the consumer will read all of the data from one partition, then move onto another partition
            // unless you have a producer with a KEY, in which case messages will be read in chronological order
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                String jsonStr = record.value();



                try {
                    JsonObject jsonObject = new JsonParser().parse(jsonStr).getAsJsonObject();
                    String timestamp = jsonObject.get("date").getAsString();
                    String geohash = jsonObject.get("geohash").getAsString();
                    String energyVal = jsonObject.get("energyVal").getAsString();
                    System.out.println(timestamp+" "+geohash+" "+energyVal+"testing");
                } catch (JsonSyntaxException e) {
                    e.printStackTrace();
                }


            }
        }


    }

}
