import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Conf {
	Properties props = null;

	String myConnectionURL = null;
	String  user = null;
	String password = null;

	//String myConnectionURL = "jdbc:oracle:thin:@10.1.10.201:1521:ORCLPDB";
	//String user = "dtsdm";
	//String password = "cL3ar#12";

	public Conf() throws IOException {
		this.props = getPropValues();
	}

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
	public void setPassword(String password) { this.password = password; }
	public Properties getProps(){ return this.props; }

	public Properties getPropValues() throws IOException {
        InputStream inputStream = null;
		Properties props = new Properties();
		try {

			String propFileName = "config.properties";

			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

			if (inputStream != null) {
				props.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}


			// get the property value and print it out

			this.myConnectionURL = props.getProperty("myConnectionURL");
			this.user = props.getProperty("user");
			this.password = props.getProperty("password");



		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
			inputStream.close();
		}
		return props;
	}
}