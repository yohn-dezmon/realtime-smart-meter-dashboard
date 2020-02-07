package redisConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class OutageRedisConsumer {

    /**
     * A consumer to push the latest instances of theft for each geohash
     *
     */

    private static String outageKey = "outageKey";

    public static void main(String[] args) throws FileNotFoundException, IOException {

        Logger logger = LoggerFactory.getLogger(OutageRedisConsumer.class.getName());
        String bootstrapServers = "localhost:9092";
        String inputTopic = "outage";
        String groupId = "outagegroup";

        CommonRedisKafka commonConsumer = new CommonRedisKafka();
        Properties properties = commonConsumer.setKafkaProperties(groupId);

        UpdateScoreTopTenRedis updateScoreRedis = new UpdateScoreTopTenRedis();

        commonConsumer.createAndRunAnomalyConsumer(inputTopic,
                properties,
                logger,
                updateScoreRedis,
                outageKey);

    }
}
