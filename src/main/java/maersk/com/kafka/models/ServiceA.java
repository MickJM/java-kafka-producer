package maersk.com.kafka.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Builder;
import lombok.Data;

//@Getter @Setter @ToString @Builder
@Data
@JsonIgnoreProperties(ignoreUnknown=true)
public class ServiceA {

	private int id;
	private String firstName;
	private String lastName;
	
	public ServiceA() {}
	
	public ServiceA(int id, String firstname, String lastname) {
		super();
		this.id = id;
		this.firstName = firstname;
		this.lastName = lastname;
	}

	
	
}
