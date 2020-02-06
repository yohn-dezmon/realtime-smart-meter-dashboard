from cassandra.cluster import Cluster
import pandas as pd
import json


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

        # list_of_lists = self.executeIndivQuery(session)
        # print(list_of_lists)
        x_and_y = self.executeIndivQuery(session, 'gcpuwuuh6xx8')
        print(x_and_y)
        # X = x_and_y[0].tolist()
        # Y = x_and_y[0].tolist()

        # print(X)
        # print(x_and_y)

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
        queryStr = "SELECT * FROM indivtimeseries where geohash = " + "'" + geohash + "'"
        try:
            rows = session.execute(queryStr)
            df = pd.DataFrame(rows)
            x = df['timestampcol']
            y = df['energy']
            # print(x.head)
            # print(y.head)
            x_and_y = [x,y]
            return x_and_y
        except:
            return 'bad_key'

if __name__ == '__main__':
    # cc stands for cassandra connector
    cc = CassandraConnector()

    cc.main()
