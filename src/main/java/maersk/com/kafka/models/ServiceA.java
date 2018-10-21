package maersk.com.kafka.models;


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

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	
	
	
}
