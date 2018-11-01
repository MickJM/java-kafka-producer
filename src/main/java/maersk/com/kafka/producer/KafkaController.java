package maersk.com.kafka.producer;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.MultiValueMap;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import maersk.com.kafka.cassandra.CassandraRepository;

@RestController
public class KafkaController {

	@Autowired
	private ApplicationContext context;
	
	@Autowired
	private ObjectMapper mapper;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	private static final MultiValueMap<String, String> headers;
	
	static {
		headers = new HttpHeaders();
		headers.put(HttpHeaders.EXPIRES, Collections.singletonList("86401"));
		headers.put(HttpHeaders.CACHE_CONTROL, Collections.singletonList("max-age=86401"));
		headers.put(HttpHeaders.CONTENT_TYPE, Collections.singletonList("application/json"));
	}

	private HttpStatus errorCode = HttpStatus.OK;
    //
    static Logger log = Logger.getLogger(KafkaController.class);
	
	public KafkaController() {
		super();
	}

	/**
	 * Post the message
	 * 
	 * @param message
	 * @param topicName
	 * @return
	 */
	@PostMapping(path="/kafka/{topicName}", consumes={MediaType.APPLICATION_JSON_UTF8_VALUE})
	public DeferredResult<ResponseEntity<String>> 
			send(@RequestBody String message, @PathVariable String topicName) {	
		
		DeferredResult<ResponseEntity<String>> result = SendMessageToKafka(topicName, message);
		return result;
		
	}

	/** 
	 * Send the message to Kafka
	 * 
	 * @param topicName
	 * @param message
	 * @return
	 */
	private DeferredResult<ResponseEntity<String>> 
			SendMessageToKafka(String topicName, String message) {

		DeferredResult<ResponseEntity<String>> result = new DeferredResult<>();
		
		ListenableFuture<SendResult<String,String>> future =
				kafkaTemplate.send(topicName, message);

		/**
		 * Add a callback to the future object to process the request
		 */
		future.addCallback(new ListenableFutureCallback<SendResult<String,String>>() {
			
			@Override
			public void onSuccess(SendResult<String,String> sendResult) {
		
				UUID uuid = UUID.randomUUID();
				
				ObjectNode resp = Success(uuid);			
				result.setResult(new ResponseEntity<> (resp.toString(), 
							headers, HttpStatus.CREATED));
				log.info("message successfully sent");
				
				log.info("Attempting to into Cassandra ...");				
				context.getBean(CassandraRepository.class).insert(uuid, message);
				
			}

			@Override
			public void onFailure(Throwable ex) {
				
			    result.setResult(new ResponseEntity<> (GetErrorResponse(ex).toString(), 
			    			headers, GetResponseStatus()));
				log.error("Error sending " + ex.getMessage());
			}
		});
		
		return result;
		
	}

	/**
	 * The message was successfully sent, create a success response message
	 * @return
	 */
	private ObjectNode Success(UUID id) {

		ObjectNode node = mapper.createObjectNode();
		node.put("id", id.toString());
		node.put("response", "success");		
		return node;
	}
	
	/**
	 * Create an error response message
	 * 
	 * @param ex
	 * @return
	 */
	private ObjectNode GetErrorResponse(Throwable ex) {

		String msg = "";
		this.errorCode = HttpStatus.INTERNAL_SERVER_ERROR;
		
	    if (ex instanceof KafkaProducerException ) {
	    	ex = ex.getCause();
        }
	     	
	    if ( ex instanceof InterruptedException ) {
        	msg = "ProcessExecutionException, " + ex.getMessage();
        	
        } else if ( ex instanceof IOException ) {
        	msg ="IOException, " + ex.getMessage();
        	this.errorCode = HttpStatus.BAD_REQUEST;

        } else if ( ex instanceof TimeoutException ) {
        	msg = "TimeoutException, " + ex.getMessage();
        	this.errorCode = HttpStatus.REQUEST_TIMEOUT;

        } else if ( ex instanceof Error ) {
        	msg = "Error, " + ex.getMessage();
        	
        } else if ( ex instanceof RuntimeException) {
        	msg = "RuntimeException, " + ex.getMessage();

        } else {
        	msg = "General error, " + ex.getMessage();

        }

		ObjectNode node = mapper.createObjectNode();
		node.put("response", "error");
		node.put("description", msg);
		
		return node;
	}

	/**
	 * Return the HTTP status code
	 * @return
	 */
	private HttpStatus GetResponseStatus() {
		return this.errorCode;
	}
	
}
