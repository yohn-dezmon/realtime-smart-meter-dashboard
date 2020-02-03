package redisConnection;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class GetTopTenRedis {
    /**
     * This class was modified from https://github.com/ironzep/JedisLeaderboard
     * JedisPool is a threadsafe pool of network connections.
     * @author Renan Geraldo
     */

    private String globalTopTen = "globalTopTen";

    public List<TopTen> getGlobalTopTenRedis() throws Exception {
        Jedis jedis = null;
        try {
            // Getting a connection from the pool
            jedis = JedisConfiguration.getPool().getResource();

            //The Tuple type has two values: key and score
            //zrevrange gets from the bigger score to the lower
            //It is being limited just for the first 100 positions
            //For all positions, pass 0 and -1
            Set<Tuple> tupleList = jedis.zrevrangeWithScores(globalTopTen, 0, 9);
            List<TopTen> topTenList = new ArrayList<TopTen>();
            long position = 0;

            //Iterating over the tuples
            for (Tuple tuple : tupleList) {
                //When using zrevrange you need to handle the positions
                position++;
                TopTen topTen = new TopTen();
                //Retrieving the user from hash set
                topTen.setHouse(this.getHouseFromHashSet(tuple.getElement(), jedis));
                //Setting the position
                topTen.setPosition(position);
                //Seting the score from the tuple
                topTen.setScore(tuple.getScore());

                //Adding the LeaderboardList
                topTenList.add(topTen);
            }
            return topTenList;
        } catch (Exception ex) {
            throw new Exception();

        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

    }

    public House getHouseFromHashSet(String geohash, Jedis jedis) {
        House house = null;

        try {
            jedis = JedisConfiguration.getPool().getResource();
            if (jedis.exists(geohash)) {
                house = new House();
                house.setGeohash(jedis.hget(geohash, "geohash"));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return house;

    }
}
