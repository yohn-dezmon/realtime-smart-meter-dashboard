package redisConnection;

public class RedisQuery {

    public static void main(String[] args) {
        UpdateScoreTopTenRedis updateScoreRedis = new UpdateScoreTopTenRedis();

        updateScoreRedis.getTopTen();
    }
}
