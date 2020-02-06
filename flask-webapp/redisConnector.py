import redis
import json
import pandas as pd
# import pdb

# pool = redis.ConnectionPool(host='')
# r = redis.Redis()
# r.mset({"Croatia": "Zagreb", "Bahamas": "Nassau"})


class RedisConnector(object):
    """This class connects to the redis server to run queries against it.
    """

    def __init__(self):
        pass

    def main(self):
        # pdb.set_trace()

        # redisIP = self.loadConfig()
        r = redis.Redis()
        # r.set('foo','bar')
        #
        # r = redis.Redis(host=redisIP, port=6379, db=0)
        # what table is this adding these to?
        # r.mset({"Croatia": "Zagreb", "Bahamas": "Nassau"})
        #
        # print(r.get("Bahamas"))

        df = self.queryForTopTenTable(r)

        print(df.to_dict('records'))

    """def loadConfig(self):
        with open('config.json') as json_data_file:
            data = json.load(json_data_file)

        # get private IP for one node of the cassandra cluster
        redisIP = data['webserver']['node1']

        return redisIP
        """

    def queryForTopTenTable(self, redisObj):

        topTenTuples = redisObj.zrevrange("globalTopTen", 0, 9, withscores=True)

        df = pd.DataFrame(topTenTuples, columns = ['Geohash','Cumulative Energy'])
        df['Geohash'] = df['Geohash'].str.decode("utf-8")
        return df







if __name__ == '__main__':
    # cc stands for cassandra connector
    rc = RedisConnector()

    rc.main()
