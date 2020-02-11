from cassandra.cluster import Cluster
import pandas as pd
import json
import geohash2
import dateutil.parser


class CassandraConnector(object):
    """This class connects to the Cassandra cluster to run queries against it.
    """

    def __init__(self):
        pass

    def main(self):
        # get private IP for one node of the cassandra cluster
        cassandraIP = self.loadConfig()

        cluster = Cluster([cassandraIP])
        # geotime = keyspace
        session = cluster.connect('geotime')


        # x_and_y = self.executeIndivQuery(session, 'gcpuwuuh6xx8')
        # print(x_and_y)

        mostRecentTimestamp = self.mostRecentTimestamp(session)

        print(mostRecentTimestamp)

        # self.get100records(session)

        strTime = '2020-02-10 13:49:46.0'
        date = dateutil.parser.parse(strTime)

        df = self.executeMapQuery(session, date)
        print(df.head)


    def getSession(self):
        cassandraIP = self.loadConfig()
        cluster = Cluster([cassandraIP])
        # geotime = keyspace
        session = cluster.connect('geotime')
        return session


    def loadConfig(self):
        with open('config.json') as json_data_file:
            data = json.load(json_data_file)

        # get private IP for one node of the cassandra cluster
        cassandraIP = data['cassandra']['node1']

        return cassandraIP

    def executeIndivQuery(self, session, geohash):
        # geohash = 'gcpuy8f1gwg5'
        if geohash == '':
            geohash = 'gcpuy8f1gwg5'
        try:
            geohash_lookup_stmt = session.prepare("SELECT * FROM indivtimeseries where geohash=?")
            rows = session.execute(geohash_lookup_stmt, [geohash])
            df = pd.DataFrame(rows)
            x = df['timestampcol']
            y = df['energy']
            # print(x.head)
            # print(y.head)
            x_and_y = [x,y]
            return x_and_y
        except:
            return 'bad_key'

    def mostRecentTimestamp(self, session):
        # SELECT timestampcol from indivtimeseries where geohash = 'v1hsg0fhpvty' ORDER BY timestampcol DESC limit 1;
        queryStr = "SELECT timestampcol FROM indivtimeseries where geohash = 'gcpuwvy45fz7' ORDER BY timestampcol DESC limit 1"
        time_lookup_stmt = session.prepare(queryStr)
        rows = session.execute(time_lookup_stmt)
        timestamp = rows[0][0]
        return timestamp

    def executeMapQuery(self, session, timestamp):
        # select * from simpletimeseries where timestampcol = '2020-02-01T16:51:03';
        # JUST FOR NOW, REMOVE THIS LATER
        # timestamp = '2020-02-07 15:04:34'
        queryStr = "SELECT geohash, energy from simpletimeseries where timestampcol=?"
        map_lookup_stmt = session.prepare(queryStr)
        rows = session.execute(map_lookup_stmt, [timestamp])
        df = pd.DataFrame(rows)

        # 'geohash', 'energy'
        df['GPS'] = df['geohash'].apply(lambda x : geohash2.decode(x))
        return df

    def get100records(self, session):
        queryStr = "select * from simpletimeseries limit 100"
        rows = session.execute(queryStr)
        df = pd.DataFrame(rows)
        # columns  = timestampcol, geohash, energy
        df.to_csv('100rowsCassandra.csv')
        # return df


if __name__ == '__main__':
    # cc stands for cassandra connector
    cc = CassandraConnector()

    cc.main()
