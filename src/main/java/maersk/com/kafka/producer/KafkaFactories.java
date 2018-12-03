package maersk.com.kafka.producer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.util.StringUtils;

/*
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.util.StringUtils;
*/

import maersk.com.kafka.cassandra.CassandraRepository;;

@Configuration
public class KafkaFactories {

	//@Value("${spring.kafka.bootstrap-servers:}")	
	// Get all the parameters
	@Value("${kafka.dest.bootstrap-servers:}")
	private String destBootstrapServers;
	@Value("${kafka.dest.username:}")
	private String destUsername;
	@Value("${kafka.dest.password:}")
	private String destPassword;
	@Value("${kafka.dest.login-module:org.apache.kafka.common.security.plain.PlainLoginModule}")
	private String destLoginModule;
	@Value("${kafka.dest.sasl-mechanism:PLAIN}")
	private String destSaslMechanism;
	@Value("${kafka.dest.truststore-location:}")
	private String destTruststoreLocation;
	@Value("${kafka.dest.truststore-password:}")
	private String destTruststorePassword;
	@Value("${kafka.dest.linger:1}")
	private int destLinger;
	
	@Value("${spring.application.name:kafka-producer}")
	private String clientId;

	@Value("${cassandra.host.name}")
	private String CassandraHost;
	@Value("${cassandra.host.port}")
	private int CassandraPort;
	
    static Logger log = Logger.getLogger(KafkaFactories.class);
	
    // Create a Bean for connections to Cassandra database
    
    @Bean
    public CassandraRepository cassandraRepository() {
    	log.info("Creating Cassandra object ...");
    	CassandraRepository cassRepos = new CassandraRepository();
    	
    	//cassRepos.connect(this.CassandraHost, this.CassandraPort);
    	//if (cassRepos.getSession() != null) {
    	//	cassRepos.createKeyspace("kafka", "SimpleStrategy", 1);
    	//}
    	return cassRepos;
    }
    
    
	@Bean
	public ProducerFactory<Object, Object> producerFactory() {
		
		LoadRunTimeParameters();
		
		Map<String, Object> properties = new HashMap<>();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destBootstrapServers);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.LINGER_MS_CONFIG, destLinger);

		properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "5000");
		properties.put("client.id", this.clientId);
		//properties.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-producer");
		properties.put("transaction.timeout.ms", 5000);
		properties.put("max.block.ms", 5000);
		properties.put("acks", "1");
		log.info("Starting producer");
		/*
		 * Testing using embedded
		 */
		//properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
		//		"org.apache.kafka.clients.producer.internals.DefaultPartitioner");
				
		
		addSaslProperties(properties, destSaslMechanism, destLoginModule, destUsername, destPassword);
		addTruststoreProperties(properties, destTruststoreLocation, destTruststorePassword);

		return new DefaultKafkaProducerFactory<>(properties);
	}
	
	private void addSaslProperties(Map<String, Object> properties, String mechanism, String loginModule, String username, String password) {
		if (!StringUtils.isEmpty(username)) {
			properties.put("security.protocol", "SASL_SSL");
			properties.put("sasl.mechanism", mechanism);
			properties.put("sasl.jaas.config",
					loginModule + " required username=" + username + " password=" + password + ";");
		}
	}

	private void addTruststoreProperties(Map<String, Object> properties, String location, String password) {
		if (!StringUtils.isEmpty(location)) {
			properties.put("ssl.truststore.location", location);
			properties.put("ssl.truststore.password", password);
		}
		properties.put("ssl.keymanager.algorithm", "SunX509");

	}
	
	private void LoadRunTimeParameters() {

    	String fileName = null;
    	try {
    		fileName = System.getenv("PARAMS");
	    	File f = new File(fileName);
	    	if (!f.exists()) {	
	    		log.info("Properties file does not exist, using defaults");
	    		return;
	    	}
    	} catch (Exception e) {
    		log.info("PARAMS parameter is missing, using defaults");
    		try {
    			this.destUsername = System.getenv("USERID");
    			this.destPassword = System.getenv("PASSWORD");
        		log.info("USERID/PASSWORD have been overriden from Envirnonment Variables");

    		} catch (Exception e1) {
    		}
    		return;    		
    	}
    	
		log.info("Using property file: " + fileName);

		Properties prop = new Properties();
		InputStream input = null;

		try {
			input = new FileInputStream(fileName);
			prop.load(input);
			
			this.destBootstrapServers = prop.getProperty("kafka.dest.bootstrap-servers");
			this.destUsername = prop.getProperty("kafka.dest.username");
			this.destPassword = prop.getProperty("kafka.dest.password");
			this.destLoginModule = prop.getProperty("kafka.dest.login-module");
			this.destSaslMechanism = prop.getProperty("kafka.dest.sasl-mechanism");
			this.destTruststoreLocation = prop.getProperty("kafka.dest.truststore-location");
			this.destTruststorePassword = prop.getProperty("kafka.dest.truststore-password");
			this.destLinger = Integer.parseInt(prop.getProperty("kafka.dest.linger"));
			this.clientId = prop.getProperty("spring.application.name");

		} catch(IOException e) {
			log.error("IOException error : " + e.getMessage());
			System.exit(1);
			
		} finally {
			
	        if (input != null) {
	            try {
	                input.close();
	                
	            } catch (IOException e) {    	
	    			// do nothing
	            
	            }
	        }
	    }
		
	}
	
}
