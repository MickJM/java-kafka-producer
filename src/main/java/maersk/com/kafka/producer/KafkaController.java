package maersk.com.kafka.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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

	@RequestMapping(value="/kafka/getA/", method=RequestMethod.GET, produces="application/json")
	//public List<ServiceA> getServiceA() {
	//https://www.baeldung.com/spring-deferred-result
	//https://www.javacodegeeks.com/2015/07/understanding-callable-and-spring-deferredresult.html
	public DeferredResult<ResponseEntity<?>> getService() {

		DeferredResult<ResponseEntity<?>> deferredResult = new DeferredResult(500l);
		
		deferredResult.onTimeout(() ->
			deferredResult.setErrorResult(
					ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT)
					.body("{\"error\":\"Request timeout occurred\"}")));
		
		deferredResult.onError((Throwable t) -> {
			deferredResult.setErrorResult(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
			        .body(GetErrorResponse(t)));
		});
		
		ForkJoinPool.commonPool().submit(() ->
		{
			ServiceA serviceA = new ServiceA(1,"Carta","TheCat");
			String resp = null;
			try {
				resp = Serialize(serviceA);
				Thread.sleep(6000);
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			deferredResult.setResult(ResponseEntity.ok(serviceA));
			
			
		});
		
		return deferredResult;
		
	}
	
	@RequestMapping(value="/kafka/get/", method=RequestMethod.GET, produces="application/json")
	//public List<ServiceA> getServiceA() {
	public ResponseEntity<?> getServiceA() {
	//public Map getServiceA() {
		List<ServiceA> list = new ArrayList<ServiceA>();
		ServiceA serviceA = new ServiceA(1,"Carta","TheCat");
		
		String resp = null;
		try {
			resp = Serialize(serviceA);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		list.add(serviceA);
		return new ResponseEntity<ServiceA>(serviceA, HttpStatus.OK);
	
	//	return Collections.singletonMap("response", serviceA);
	}
	
	// http://websystique.com/spring-boot/spring-boot-rest-api-example/
	
	@PostMapping(path="/kafka/{topicName}", consumes={MediaType.APPLICATION_JSON_VALUE})
	public DeferredResult<ResponseEntity<String>> 
			send(@RequestBody String message, @PathVariable String topicName) {	

		DeferredResult<ResponseEntity<String>> result = SendMessageToKafka(topicName, message);
		return result;
		
	}

	private DeferredResult<ResponseEntity<String>> SendMessageToKafka(String topicName, String message) {

		DeferredResult<ResponseEntity<String>> result = new DeferredResult<>();
		
		try {
			DeSerialize(message);
			
		} catch (JsonParseException e) {
		    result.setResult(new ResponseEntity<> (GetErrorResponse(e).toString(), headers, GetResponseStatus()));
			log.info("JsonParseException");
			return result;
			
		} catch (JsonMappingException e) {
		    result.setResult(new ResponseEntity<> (GetErrorResponse(e).toString(), headers, GetResponseStatus()));
			log.info("JsonMappingException");
			return result;
			
		} catch (IOException e) {
		    result.setResult(new ResponseEntity<> (GetErrorResponse(e).toString(), headers, GetResponseStatus()));
			log.info("IOException");
			return result;
		}

		
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
	
	private String Serialize(ServiceA service) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper  mapper = new ObjectMapper();
		service.setId(Integer.parseInt("ABC"));
		String json = mapper.writeValueAsString(service);
		log.info("Serialized");
		return json;
	}

}
