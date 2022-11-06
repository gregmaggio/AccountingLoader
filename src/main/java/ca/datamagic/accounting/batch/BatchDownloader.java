/**
 * 
 */
package ca.datamagic.accounting.batch;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ca.datamagic.accounting.dao.FileDAO;

/**
 * @author gregm
 *
 */
public class BatchDownloader {
	private static final Logger logger = LogManager.getLogger(BatchDownloader.class);
	
	/**
	 * Download a file for a date or all log files
	 * @param args
	 */
	public static void main(String[] args) {		
		FileDAO dao = new FileDAO();
		try {
			String date = null;
			for (int ii = 0; ii < args.length;) {
				String arg = args[ii++];
				if (arg.toLowerCase().contains("date")) {
					if (ii < args.length) {
						date = args[ii++];
						continue;
					}
				}
			}
			logger.debug("date: " + date);
			
			dao.connect();
			
			String[] logFiles = dao.getLogFiles();
			for (int ii = 0; ii < logFiles.length; ii++) {
				String logFile = logFiles[ii];
				logger.debug("logFile: " + logFile);
				if (date != null) {
					if (logFile.contains(date)) {
						dao.downloadLogFile(logFile);
					}
					continue;
				}
				dao.downloadLogFile(logFile);
			}
		} catch (Throwable t) {
			logger.error("Throwable", t);
		}
		try {
			dao.disconnect();
		} catch (IOException ex) {
			logger.warn("IOException", ex);
		}
	}

}
