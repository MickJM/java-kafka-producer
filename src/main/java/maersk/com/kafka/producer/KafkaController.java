package maersk.com.kafka.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.MultiValueMap;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import maersk.com.kafka.models.*;

@RestController
public class KafkaController {

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

	@GetMapping("/kafka/get/")
	public List<ServiceA> getServiceA() {
	
		List<ServiceA> serviceA = new ArrayList<ServiceA>();
		serviceA.add(new ServiceA(1,"Carta","TheCat"));
		return serviceA;
	}
	
	@PostMapping("/kafka/{topicName}")
	public DeferredResult<ResponseEntity<String>> 
			send(@RequestBody String message, @PathVariable String topicName) {	

		try {
			DeSerialize(message);
			
		} catch (JsonParseException e) {
			log.info("JsonParseException");
			return null;
			
		} catch (JsonMappingException e) {
			log.info("JsonMappingException");
			return null;
			
		} catch (IOException e) {
			log.info("IOException");
			return null;
		}

		DeferredResult<ResponseEntity<String>> result = SendMessageToKafka(topicName, message);
		return result;
		
	}

	private DeferredResult<ResponseEntity<String>> SendMessageToKafka(String topicName, String message) {

		DeferredResult<ResponseEntity<String>> result = new DeferredResult<>();
		
	//	ListenableFuture<SendResult<String,String>> future =
	//			kafkaSender.send(topicName == null ? defaultTopic : topicName, message);
		ListenableFuture<SendResult<String,String>> future =
				kafkaTemplate.send(topicName, message);

		//future.addCallback(cb);
		
		
		future.addCallback(new ListenableFutureCallback<SendResult<String,String>>() {
			
			@Override
			public void onSuccess(SendResult<String,String> sendResult) {
		
				ObjectNode resp = Success();			
				result.setResult(new ResponseEntity<> (resp.toString(), headers, HttpStatus.CREATED));
				log.info("message successfully sent");
			}

			@Override
			public void onFailure(Throwable ex) {
				
			//	ObjectNode resp = GetErrorResponse(ex);		
			    result.setResult(new ResponseEntity<> (GetErrorResponse(ex).toString(), headers, GetResponseStatus()));
				log.error("Error sending " + ex.getMessage());
			}
		});
		
		return result;
		
	}

	//@GetMapping
	private ObjectNode Success() {

		ObjectNode node = mapper.createObjectNode();
		node.put("response", "success");		
		return node;
	}
	
	
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
	
	private HttpStatus GetResponseStatus() {
		return this.errorCode;
	}
	
	private void DeSerialize(String msg) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper  mapper = new ObjectMapper();
		//mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		ServiceA A = mapper.readValue(msg, ServiceA.class);
		log.info("Deserialized");
	}
}
