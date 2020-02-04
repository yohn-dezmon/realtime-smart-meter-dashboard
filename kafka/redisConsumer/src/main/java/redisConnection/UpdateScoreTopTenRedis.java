package redisConnection;

import redis.clients.jedis.Jedis;

public class UpdateScoreTopTenRedis {
    /**
     * This class was modified from https://github.com/ironzep/JedisLeaderboard
     * JedisPool is a threadsafe pool of network connections.
     * @author Renan Geraldo
     */
    private String globalTopTen = "globalTopTen";

    public void updateScore(String geohash, double score) {
        Jedis jedis = null;
        try {
            // Getting a connection from the pool
            jedis = JedisConfiguration.getPool().getResource();

            // Updating the user score with zadd. With zadd you overwrite the score.
            jedis.zadd(globalTopTen, score, geohash);


        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

    }
}
