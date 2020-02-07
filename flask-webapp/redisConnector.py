import redis
import json
import pandas as pd


class RedisConnector(object):
    """This class connects to the redis server to run queries against it.
    """

    def __init__(self):
        pass

    def main(self):
        r = redis.Redis()
        df = self.queryForTopTenTable(r)

        outageKey = "outageKey"
        df = self.queryForAnomalyTables(r,outageKey)


        print(df.to_dict('records'))



    def queryForTopTenTable(self, redisObj):

        topTenTuples = redisObj.zrevrange("globalTopTen", 0, 9, withscores=True)

        df = pd.DataFrame(topTenTuples, columns = ['Geohash','Cumulative Energy'])
        df['Geohash'] = df['Geohash'].str.decode("utf-8")
        return df

    def queryForAnomalyTables(self, redisObj, rediskey):

        tenMostRecentAnomalies = redisObj.zrevrange(rediskey, 0,9, withscores=True)

        df = pd.DataFrame(tenMostRecentAnomalies, columns = ['Geohash', 'Timestamp'])
        df['Geohash'] = df['Geohash'].str.decode("utf-8")
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms').dt.strftime('%B %d, %Y, %r')
        return df

if __name__ == '__main__':
    # cc stands for cassandra connector
    rc = RedisConnector()

    rc.main()
