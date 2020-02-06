package redisConnection;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Set;

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

    public void getTopTen() {
        // querying the DB
        Jedis jedis = null;
        try {
            // getting a connection from the pool
            jedis = JedisConfiguration.getPool().getResource();

            // getting the top 10 with their scores...
            // this doesn't need to be time based, just select top 10
            // > zrevrange globalTopTen 0 9 WITHSCORES
            Set<Tuple> sets = jedis.zrevrangeWithScores(globalTopTen, 0, 9);
                int i = 0;
                for (Tuple set : sets) {
                    System.out.println(set.getElement()+ " " + set.getScore());
                    i++;
                }


        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
