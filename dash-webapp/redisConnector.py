import redis
import json
import pandas as pd


class RedisConnector(object):
    """This class connects to the redis server to run queries against it."""

    def __init__(self):
        pass

    def main(self):
        pass

    def queryForTopTenTable(self, redisObj):
        """ This runs the query to extract the top 10 geohashes by cumulative energy.
        This data is put into redis from the cumulative sum Kafka streams application.
        """

        # zrevrange is the method to get a sorted set from redis
        topTenTuples = redisObj.zrevrange("globalTopTen", 0, 9, withscores=True)

        # convert query results to dataframe
        df = pd.DataFrame(topTenTuples, columns = ['Geohash','Cumulative Energy'])
        df['Geohash'] = df['Geohash'].str.decode("utf-8")
        return df

    def queryForAnomalyTables(self, redisObj, rediskey):
        """ This finds the top 10 of the most recent anomalous data (either outages or theft)."""
        tenMostRecentAnomalies = redisObj.zrevrange(rediskey, 0,9, withscores=True)

        df = pd.DataFrame(tenMostRecentAnomalies, columns = ['Geohash', 'Timestamp'])
        df['Geohash'] = df['Geohash'].str.decode("utf-8")
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms').dt.strftime('%B %d, %Y, %r')
        return df

if __name__ == '__main__':
    # rc stands for redis connector
    rc = RedisConnector()

    rc.main()
