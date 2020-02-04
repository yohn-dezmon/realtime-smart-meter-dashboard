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

        list_of_lists = self.executeIndivQuery(session)
        print(list_of_lists)

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

    def executeIndivQuery(self, session):
        # geohash = 'gcpuy8f1gwg5'
        rows = session.execute("SELECT * FROM indivtimeseries where geohash = 'gcpuy8f1gwg5'")
        list_of_lists = []
        for row in rows:
            rowlist = [row.geohash, row.timestampcol, row.energy]
            list_of_lists.append(rowlist)
        return list_of_lists

if __name__ == '__main__':
    # cc stands for cassandra connector
    cc = CassandraConnector()

    cc.main()
