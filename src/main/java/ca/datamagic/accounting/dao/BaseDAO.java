/**
 * 
 */
package ca.datamagic.accounting.dao;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.Lists;

/**
 * @author gregm
 *
 */
public abstract class BaseDAO {
	private Properties properties = null;
	private GoogleCredentials credentials = null;
	
	public Properties getProperties() throws IOException {
		if (this.properties == null) {
			InputStream inputStream = null;
			try {
				inputStream = getClass().getClassLoader().getResourceAsStream("secure.properties");
				Properties properties = new Properties();
				properties.load(inputStream);
				this.properties = properties;
			} finally {
				if (inputStream != null) {
					inputStream.close();
				}
			}
		}
		return this.properties;
	}
	
	public GoogleCredentials getCredentials() throws IOException {
		if (this.credentials == null) {
			InputStream inputStream = null;
			try {
				inputStream = getClass().getClassLoader().getResourceAsStream("secure.json");
				this.credentials = GoogleCredentials.fromStream(inputStream).createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
			} finally {
				if (inputStream != null) {
					inputStream.close();
				}
			}
		}
		return this.credentials;
	}
}
