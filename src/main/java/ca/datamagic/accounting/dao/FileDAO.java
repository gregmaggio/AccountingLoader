/**
 * 
 */
package ca.datamagic.accounting.dao;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ProtocolCommandEvent;
import org.apache.commons.net.ProtocolCommandListener;
import org.apache.commons.net.ftp.FTPClient;

/**
 * @author gregm
 *
 */
public class FileDAO extends BaseDAO {
	private static final Logger logger = Logger.getLogger(FileDAO.class.getName());
	private static String tempPath = "C:/Temp";
	private FTPClient client = null;
	
	public static String getTempPath() {
		return tempPath;
	}
	
	public static void setTempPath(String newVal) {
		tempPath = newVal;
	}
	
	public void connect() throws IOException {
		if (this.client == null) {
			this.client = new FTPClient();
			this.client.addProtocolCommandListener(new ProtocolCommandListener() {
			      @Override
			      public void protocolCommandSent(ProtocolCommandEvent event) {
			    	  logger.info("protocolCommandSent");
			    	  logger.info("Command: " + event.getCommand());
			    	  logger.info("Message: " + event.getMessage());
			      }

			      @Override
			      public void protocolReplyReceived(ProtocolCommandEvent event) {
			    	  logger.info("protocolReplyReceived");
			    	  String command = event.getCommand();
			    	  String message = event.getMessage();
			    	  if (message.contains("PASS")) {
			    		  message = "PASS XXXXXXXX";
			    	  }
			    	  logger.info("Command: " + command);
			    	  logger.info("Message: " + message);
			      }
			});
			this.client.connect(getFTPHost());
			boolean loginResult = this.client.login(getFTPUser(), getFTPPass());
			logger.info("loginResult: " + loginResult);
			this.client.enterLocalPassiveMode();
			String workingDirectory = this.client.printWorkingDirectory();
			logger.info("workingDirectory: " + workingDirectory);
			/*
			FTPFile[] directories = this.client.listDirectories();
			logger.debug("replyString: " + this.client.getReplyString());
			logger.debug("directories: " + directories);
			if (directories != null) {
				for (int ii = 0; ii < directories.length; ii++) {
					logger.debug("name: " + directories[ii].getName());
				}
			}
			FTPFile[] files = this.client.listFiles();
			logger.debug("replyString: " + this.client.getReplyString());
			logger.debug("files: " + files);
			if (files != null) {
				for (int ii = 0; ii < files.length; ii++) {
					logger.debug("name: " + files[ii].getName());
				}
			}
			int cmdResult = this.client.sendCommand(FTPCmd.LIST);
			logger.debug("cmdResult: " + cmdResult);
			logger.debug("replyString: " + this.client.getReplyString());
			*/
		}
	}
	
	public void disconnect() throws IOException {
		if (this.client != null) {
			this.client.disconnect();
		}
		this.client = null;
	}
	
	public String getCSVDirectory() {
		return MessageFormat.format("{0}/csv", getTempPath());
	}
	
	public String getAVRODirectory() {
		return MessageFormat.format("{0}/avro", getTempPath());
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
	
	public int getBufferSize() throws IOException {
		return Integer.parseInt(this.getProperties().getProperty("bufferSize"));
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
		if (files != null) {
			Arrays.sort(files, comparator);
		}
		return files;
	}
	
	public String[] getCSVFiles() throws IOException {
		return getFiles(getCSVDirectory(), "accounting.csv");
	}
	
	public String[] getAVROFiles() throws IOException {
		return getFiles(getAVRODirectory(), "accounting.avro");
	}
	
	public String[] getLogFiles() throws IOException {
		String ftpDirectory = getFTPDirectory();
		logger.info("ftpDirectory: " + ftpDirectory);
		String[] fileNames = this.client.listNames(ftpDirectory);		
		logger.info("replyString: " + this.client.getReplyString());
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
		logger.info("logFile: " + logFile);
		String fileName = logFile.substring(logFile.lastIndexOf("/") + 1);
		logger.info("fileName: " + fileName);
		String downloadFile = MessageFormat.format("{0}/{1}", getCSVDirectory(), fileName);
		logger.info("downloadFile: " + downloadFile);
		FileOutputStream outputStream = new FileOutputStream(downloadFile);
		this.client.retrieveFile(logFile, outputStream);
		outputStream.close();
	}
	
	public void downloadLogFileForDate(String date) throws IOException {
		InputStream remoteStream = null;
		OutputStream localStream = null;
		try {
			logger.info("date: " + date);
			String fileName = MessageFormat.format("accounting.csv.{0}", date);
			logger.info("fileName: " + fileName);
			String remoteFile = MessageFormat.format("{0}/{1}", getFTPDirectory(), fileName);
			logger.info("remoteFile: " + remoteFile);
			String localFile = MessageFormat.format("{0}/{1}", getCSVDirectory(), fileName);
			logger.info("localFile: " + localFile);
			remoteStream = this.client.retrieveFileStream(remoteFile);
			if (remoteStream != null) {
				localStream = new FileOutputStream(localFile);
				byte[] buffer = new byte[getBufferSize()];
				int bytesRead = 0;
				while ((bytesRead = remoteStream.read(buffer, 0, buffer.length)) > 0) {
					localStream.write(buffer, 0, bytesRead);
				}
			}
		} finally {
			if (remoteStream != null) {
				try {
					remoteStream.close();
				} catch (IOException ex) {
					logger.warning("IOException: " + ex.getMessage());
				}
			}
			if (localStream != null) {
				try {
					localStream.close();
				} catch (IOException ex) {
					logger.warning("IOException: " + ex.getMessage());
				}
			}
		}
	}
	
	public void deleteCSVFile(String date) {
		String fileName = MessageFormat.format("{0}/accounting.csv.{1}", getCSVDirectory(), date);
		logger.info("fileName: " + fileName);
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
		}
	}
	
	public void deleteCSVFiles() throws IOException {
		FileUtils.cleanDirectory(new File(getCSVDirectory()));
	}
	
	public void deleteAVROFile(String date) {
		String fileName = MessageFormat.format("{0}/accounting.avro.{1}", getAVRODirectory(), date);
		logger.info("fileName: " + fileName);
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
		}
	}
	
	public void deleteAVROFiles() throws IOException {
		FileUtils.cleanDirectory(new File(getAVRODirectory()));
	}
}
