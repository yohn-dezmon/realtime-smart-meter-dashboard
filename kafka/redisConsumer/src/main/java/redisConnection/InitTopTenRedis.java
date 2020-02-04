package redisConnection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

public class InitTopTenRedis {
    /**
     * This class was modified from https://github.com/ironzep/JedisLeaderboard
     * JedisPool is a threadsafe pool of network connections.
     * @author Renan Geraldo
     */
    private String globalTopTen = "globalTopTen";

    public void jedisInit(List<House> houseList) {
        Jedis jedis = null;
        try {

            // Getting a connection from the pool
            jedis = JedisConfiguration.getPool().getResource();

            Map<String, Double> map = new HashMap<String, Double>();

            // For each user, creates a hash set and put in a map
            for (House house : houseList) {
                map.put(house.getGeohash(), 0.0);
//                jedis.hset(user.getId(), "id", user.getId());
//                jedis.hset(user.getId(), "username", user.getUsername());

            }

            // Adding all the keys(userId) related to value(score) in our sorted list
            jedis.zadd(globalTopTen, map);

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            if (jedis != null) {
                // Closing connection to
                jedis.close();
            }
        }

    }
}
