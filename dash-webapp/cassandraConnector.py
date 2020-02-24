from cassandra.cluster import Cluster
import pandas as pd
import json
import geohash2
import dateutil.parser


class CassandraConnector(object):
    """This class connects to the Cassandra cluster to run queries against it."""

    def __init__(self):
        pass

    def main(self):
        pass

    def getSession(self):
        """ Gets access to the geotime KeySpace in cassandra. """
        cassandraIP = self.loadConfig()
        cluster = Cluster([cassandraIP])
        # geotime = keyspace
        session = cluster.connect('geotime')
        return session


    def loadConfig(self):
        """ Loads the IP for the cassandra cluster."""
        with open('config.json') as json_data_file:
            data = json.load(json_data_file)

        # get private IP for one node of the cassandra cluster
        cassandraIP = data['cassandra']['node1']

        return cassandraIP

    def executeIndivQuery(self, session, geohash):
        """ This query extracts the timeseries data for an inidivual geohash. """
        # this is the default geohash incase one is not provided by the user
        if geohash == '':
            geohash = 'gcpuy8f1gwg5'
        try:
            geohash_lookup_stmt = session.prepare("SELECT geohash, timestampcol, energy FROM indivtimeseries where geohash=?")
            rows = session.execute(geohash_lookup_stmt, [geohash])
            df = pd.DataFrame(rows)
            x = df['timestampcol']
            y = df['energy']
            x_and_y = [x,y]
            return x_and_y
        except:
            return 'bad_key'

    def mostRecentTimestamp(self, session):
        """This query selects the most recent timestamp associated with a given geohash
        in order to be able to supply a recent timestamp to the map query
        another example geohash: 'v1hsg0fhpvty'
        another example geohash: 'gcpuwvy45fz7'
        """
        queryStr = "SELECT timestampcol FROM indivtimeseries where geohash = 'gcpuy8f1gwg5' ORDER BY timestampcol DESC limit 1"
        time_lookup_stmt = session.prepare(queryStr)
        rows = session.execute(time_lookup_stmt)
        timestamp = rows[0][0]
        return timestamp

    def executeMapQuery(self, session, timestamp):
        """This query selects all of the data for the 10,000 households
        at a given timestamp. It also decodes the geohash to a GPS tuple.
        # example timestamp in ISO 8601 format = '2020-02-01T16:51:03';
        # example timestamp = '2020-02-07 15:04:34'
        """

        queryStr = "SELECT geohash, energy from simpletimeseries where timestampcol=?"
        map_lookup_stmt = session.prepare(queryStr)
        rows = session.execute(map_lookup_stmt, [timestamp])
        df = pd.DataFrame(rows)

        # 'geohash', 'energy'
        df['GPS'] = df['geohash'].apply(lambda x : geohash2.decode(x))
        df['lat'] = df['GPS'].apply(lambda x : x[0])
        df['lon'] = df['GPS'].apply(lambda x : x[1])
        return df

    def get100records(self, session):
        """ This method was used for test purposes."""
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
