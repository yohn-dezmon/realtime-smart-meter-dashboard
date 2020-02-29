package redisConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisConfiguration {
    /**
     * This class was taken from https://github.com/ironzep/JedisLeaderboard
     * JedisPool is a threadsafe pool of network connections.
     * @author Renan Geraldo
     */
    private static JedisPool jedisPool;

    public static JedisPool getPool() throws FileNotFoundException, IOException {
        String basePath = new File("").getAbsolutePath();
        String pathToProps = basePath+"/private.properties";

        Properties props2 = new Properties();
        FileInputStream fis = new FileInputStream(pathToProps);
        props2.load(fis);
        String redisIP = props2.getProperty("webserverIP");

        if (JedisConfiguration.jedisPool == null) {

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            // The max objects to be allocated by the pool. If total is reached, the pool is
            // exhausted.
            poolConfig.setMaxTotal(128);

            // The max objects to be idle in the pool
            poolConfig.setMaxIdle(128);

            // The min objects to be idle in the pool
            poolConfig.setMinIdle(16);

            // The minimum time that an object can be idle in the pool before eviction due
            // to idle
            poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());

            // The time for the thread which checks the idle objects to sleep
            poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());

            // It is going to block the connections until an idle instance becomes available
            poolConfig.setBlockWhenExhausted(true);

            //Creating Jedis Pool with default port 6379
            JedisConfiguration.jedisPool = new JedisPool(poolConfig, redisIP);
        }

        return JedisConfiguration.jedisPool;
    }

}