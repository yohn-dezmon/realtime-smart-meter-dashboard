import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonCassandra {
    // This class will be used for the cassandra connecting/schema
    // creation functions that are common amongst all consumers

    private Cluster cluster;
    private Session session;


    private String KEYSPACE;
    private static final Logger LOG = LoggerFactory.getLogger(CommonCassandra.class);


    public CommonCassandra(String KEYSPACE) {
        this.KEYSPACE = KEYSPACE;
    }

    // connect to Cassandra
    public void connect(String node, Integer port) {

        Cluster.Builder b = Cluster.builder().addContactPoint(node);

        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();

        Metadata metadata = cluster.getMetadata();
        LOG.info("Cluster name: " + metadata.getClusterName());

        session = cluster.connect();

    }

    // get cassandra session
    public Session getSession() {
        return this.session;
    }

    // close cassandra connection
    public void close() {
        session.close();
        cluster.close();
    }

    // Creating Cassandra KeySpace
    public void createKeySpace(
            String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(keyspaceName).append(" WITH replication = {")
                        .append("'class':'").append(replicationStrategy)
                        .append("','replication_factor':").append(replicationFactor)
                        .append("};");

        String query = sb.toString();
        session.execute(query);

    }

    public void useKeyspace(String keyspace) {
        session.execute("USE " + keyspace);
    }

    public void createIndividualTimeSeriesTable(String tableName) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append("(")
                .append("geohash text, timestampcol timestamp, energy double, PRIMARY KEY(geohash, timestampcol)) ")
                .append("WITH CLUSTERING ORDER BY (timestampcol ASC);");

        final String query = sb.toString();
        session.execute(query);
    }

    public void createTimeSeriesTable(String tableName) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append("(")
                .append("geohash text, timestampcol timestamp, energy double, PRIMARY KEY(timestampcol, geohash)) ")
                .append("WITH CLUSTERING ORDER BY (geohash ASC);");

        final String query = sb.toString();
        session.execute(query);
    }

    public void createCumulativeSumTable(String tableName) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append("(")
                .append("geohash text, energy double, PRIMARY KEY(geohash))")
                .append(";");

        final String query = sb.toString();
        session.execute(query);
    }

    public void insertToIndividualTimeSeriesTable(String geohash,
                                                  String timestamp,
                                                  String energy,
                                                  String keyspaceTable) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(keyspaceTable).append(" (geohash, timestampcol, energy) ")
                .append("VALUES ('").append(geohash)
                .append("', '").append(timestamp)
                .append("', ").append(energy).append(");");

        String query = sb.toString();
        session.execute(query);
    }

    public void insertToTimeSeriesTable(String geohash,
                                        String timestamp,
                                        String energy,
                                        String keyspaceTable) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(keyspaceTable).append(" (geohash, timestampcol, energy) ")
                .append("VALUES ('").append(geohash)
                .append("', '").append(timestamp)
                .append("', ").append(energy).append(");");

        String query = sb.toString();
        session.execute(query);
    }

    public void insertToCumulativeSumTable(String geohash,
                                           Double energy,
                                           String keyspaceTable) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(keyspaceTable).append(" (geohash, energy) ")
                .append("VALUES ('").append(geohash)
                .append("', ").append(energy).append(");");

        String query = sb.toString();
        session.execute(query);
    }



}
