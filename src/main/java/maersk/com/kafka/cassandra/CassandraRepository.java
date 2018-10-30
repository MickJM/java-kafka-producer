package maersk.com.kafka.cassandra;

import java.util.UUID;

import javax.annotation.PreDestroy;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ExecutionInfo;

import maersk.com.kafka.producer.KafkaController;

import com.datastax.driver.core.Session;

// https://www.baeldung.com/cassandra-with-java

@Component
public class CassandraRepository {

	//@Autowired
	//private ApplicationContext context;

	private Cluster cluster;
	private Session session;
	
    public static Logger log = Logger.getLogger(CassandraRepository.class);

	public CassandraRepository() {}
	
	public void connect(String node, Integer port) {
        Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();
        session = cluster.connect();
    }
	
	public Session getSession() {
        return this.session;
    }
 
	@PreDestroy
	public void close() {
		log.info("Closing Cassandra session");
		if (session != null) {
			session.close();
		}

		log.info("Closing Cassandra cluster connection");
		if (cluster != null) {
			cluster.close();
		}
	}
	
	public void createKeyspace(
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
	
	public void insert(UUID id, String msg) {
		
		log.info("Inserting into Cassandra database ");
		//String json = "{\"request\":\"service2\"}";
		
		StringBuilder sb = new StringBuilder("INSERT INTO ")
				.append("kafka.service1 ").append("(id, value) ")
				.append("VALUES (").append(id.toString())
				.append(", ").append("textAsBlob('").append(msg)
				//.append("textAsBlob('").append(json)
				.append("'));");
		
		String query = sb.toString();
		try {
			session.execute(query);
			
		} catch (Exception e)
		{
			System.out.println("Error updating cassandra");
		}
		int i = 0;

	}
}
