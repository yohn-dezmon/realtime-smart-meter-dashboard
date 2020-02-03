package redisConnection;

public class House {
    /** Custom Java Object representing a house identified by a geohash
     to be converted into a redis sorted set  */
    private String geohash;

    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

}
