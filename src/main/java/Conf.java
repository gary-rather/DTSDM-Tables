

public class Conf {
	String myConnectionURL = "jdbc:oracle:thin:@172.19.129.63:1521:DDCD";
	String user = "dtsdm";
	String password = "Or#cl3cL3ar#12";
	public String getMyConnectionURL() {
		return myConnectionURL;
	}
	public void setMyConnectionURL(String myConnectionURL) {
		this.myConnectionURL = myConnectionURL;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	
	
}