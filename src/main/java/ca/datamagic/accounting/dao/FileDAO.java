/**
 * 
 */
package ca.datamagic.accounting.dao;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author gregm
 *
 */
public class FileDAO extends BaseDAO {
	private static final Logger logger = LogManager.getLogger(FileDAO.class);
	private FTPClient client = null;
	
	public void connect() throws IOException {
		if (this.client == null) {
			this.client = new FTPClient();
			this.client.connect(getFTPHost());
			this.client.login(getFTPUser(), getFTPPass());
		}
	}
	
	public void disconnect() throws IOException {
		if (this.client != null) {
			this.client.disconnect();
		}
		this.client = null;
	}
	
	public String getCSVDirectory() throws IOException {
		return this.getProperties().getProperty("csvDirectory");
	}
	
	public String getAVRODirectory() throws IOException {
		return this.getProperties().getProperty("avroDirectory");
	}
	
	public String getLoadedDirectory() throws IOException {
		return this.getProperties().getProperty("loadedDirectory");
	}
	
	public String getFTPHost() throws IOException {
		return this.getProperties().getProperty("ftpHost");
	}
	
	public String getFTPUser() throws IOException {
		return this.getProperties().getProperty("ftpUser");
	}
	
	public String getFTPPass() throws IOException {
		return this.getProperties().getProperty("ftpPass");
	}
	
	public String getFTPDirectory() throws IOException {
		return this.getProperties().getProperty("ftpDirectory");
	}
	
	private String[] getFiles(String directoryName, final String filter) throws IOException {
		File inputDirectory = new File(directoryName);
		FilenameFilter filenameFilter = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if (filter == "*.*") {
					return true;
				}
				return (name.toLowerCase().contains(filter));
			}
		};
		String[] files = inputDirectory.list(filenameFilter);
		Comparator<String> comparator = new Comparator<String>() {			
			@Override
			public int compare(String o1, String o2) {
				if ((o1 != null) && (o2 != null)) {
					return o1.compareToIgnoreCase(o2);
				}
				return 0;
			}
		};
		Arrays.sort(files, comparator);
		return files;
	}
	
	public String[] getCSVFiles() throws IOException {
		return getFiles(getCSVDirectory(), "accounting.csv");
	}
	
	public String[] getAVROFiles() throws IOException {
		return getFiles(getAVRODirectory(), "accounting.avro");
	}
	
	public String[] getLogFiles() throws IOException {
		String[] fileNames = this.client.listNames(getFTPDirectory());
		List<String> logFiles = new ArrayList<>();
		for (int ii = 0; ii < fileNames.length; ii++) {
			String fileName = fileNames[ii];
			if (fileName.contains("accounting.csv.")) {
				logFiles.add(fileName);
			}
		}
		String[] array = new String[logFiles.size()];
		logFiles.toArray(array);
		Arrays.sort(array);
		return array;
	}
	
	public void downloadLogFile(String logFile) throws IOException {
		logger.debug("logFile: " + logFile);
		String fileName = logFile.substring(logFile.lastIndexOf("/") + 1);
		logger.debug("fileName: " + fileName);
		String downloadFile = MessageFormat.format("{0}/{1}", getCSVDirectory(), fileName);
		logger.debug("downloadFile: " + downloadFile);
		FileOutputStream outputStream = new FileOutputStream(downloadFile);
		this.client.retrieveFile(logFile, outputStream);
		outputStream.close();
	}
}
