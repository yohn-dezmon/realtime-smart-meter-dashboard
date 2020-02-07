package redisConnection;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommonRedisKafka {

    public static Properties setKafkaProperties(String groupId) {
        String bootstrapServers = "localhost:9092";

        // New consumer configs (Kafka docs)
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // "earliest/latest/none"

        return properties;
    }

    public static void createAndRunAnomalyConsumer(String inputTopic,
                                                   Properties properties,
                                                   Logger logger,
                                                   UpdateScoreTopTenRedis updateScoreRedis,
                                                   String rediskey) {
        // create consumer
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(properties);

        Pattern regexP = Pattern.compile("([a-zA-Z0-9]+)(.*)");

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(inputTopic));



        // poll for new data
        while (true) {
            // set language to 8
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                String geohashKey = "a";
                Matcher m = regexP.matcher(record.key().toString());
                if (m.find()) {
                    geohashKey = m.group(1);
                    System.out.println(geohashKey);
                }

                // Value:0.00061,false,false,2020-02-04 14:15:34.918,
                String[] arr = record.value().split(",");
                String timestampstr = arr[3];
                logger.info(timestampstr);

                try {
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                   Timestamp timestamp = Timestamp.valueOf(timestampstr);
                    long milliseconds = timestamp.getTime();
                    updateScoreRedis.updateAnomalyScore(geohashKey, milliseconds, rediskey);
                } catch(Exception e) {
                    e.printStackTrace();
                }


            }

        }


    }
}
