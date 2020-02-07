package redisConnection;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TheftRedisConsumer {
    /**
     * A consumer to push the latest instances of theft for each geohash
     *
     */

    private static String theftKey = "theftKey";

    public static void main(String[] args) throws FileNotFoundException, IOException {

        Logger logger = LoggerFactory.getLogger(TheftRedisConsumer.class.getName());
        String bootstrapServers = "localhost:9092";
        String inputTopic = "theft";
        String groupId = "theftgroup";

        CommonRedisKafka commonConsumer = new CommonRedisKafka();
        Properties properties = commonConsumer.setKafkaProperties(groupId);

        UpdateScoreTopTenRedis updateScoreRedis = new UpdateScoreTopTenRedis();

        commonConsumer.createAndRunAnomalyConsumer(inputTopic,
                properties,
                logger,
                updateScoreRedis,
                theftKey);

    }

}
