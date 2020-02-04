from cassandra.cluster import Cluster
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

        self.executeIndivQuery(session)


    def loadConfig(self):
        with open('config.json') as json_data_file:
            data = json.load(json_data_file)

        # get private IP for one node of the cassandra cluster
        cassandraIP = data['cassandra']['node1']

        return cassandraIP

    def executeIndivQuery(self, session):
        # geohash = 'gcpuy8f1gwg5'
        rows = session.execute("SELECT * FROM indivtimeseries where geohash = 'gcpuy8f1gwg5'")
        for row in rows:
            print(row.geohash, row.timestampcol, row.energy)

if __name__ == '__main__':
    # cc stands for cassandra connector
    cc = CassandraConnector()

    cc.main()
