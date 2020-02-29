package redisConnection;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class RedisConsumer {
    /**
     * A consumer to add the cumulative sum of energy for each geohash to
     * redis to later retrieve the top 10 consumers of energy over all time.
     */
    public static void main(String[] args) throws FileNotFoundException, IOException {
//        JedisConfiguration jedisConfiguration = new JedisConfiguration();
//        JedisPool jPool = jedisConfiguration.getPool();
        UpdateScoreTopTenRedis updateScoreRedis = new UpdateScoreTopTenRedis();

        String bootstrapServers = "localhost:9092";
        String groupId = "redistopten";
        String inputTopic = "cumulativesum";

        // New consumer configs (Kafka docs)
        Properties properties = new Properties();
        Logger logger = LoggerFactory.getLogger(RedisConsumer.class.getName());

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // "earliest/latest/none"

        // create consumer
        KafkaConsumer<String, Double> consumer =
                new KafkaConsumer<String, Double>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(inputTopic));

        // poll for new data
        while (true) {
            // set language to 8
            ConsumerRecords<String, Double> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, Double> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                updateScoreRedis.updateScore(record.key(), record.value());
                //time stamp value (time the measurement was submitted to DB!)
                Instant now = Instant.now();

                String tsString = now.toString();
                logger.info("TIME PUT IN DB: " + tsString);
            }

        }
    }

}
