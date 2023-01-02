/**
 * 
 */
package ca.datamagic.accounting.batch;

import java.io.IOException;
import java.util.logging.Logger;

import ca.datamagic.accounting.dao.FileDAO;

/**
 * @author gregm
 *
 */
public class BatchDownloader {
	private static final Logger logger = Logger.getLogger(BatchDownloader.class.getName());
	
	public static void download(String date) throws IOException {
		logger.info("date: " + date);
		FileDAO dao = new FileDAO();
		try {
			dao.connect();
			dao.downloadLogFileForDate(date);
		} finally {
			dao.disconnect();
		}
	}
	
	/**
	 * Download a file for a date or all log files
	 * @param args
	 */
	public static void main(String[] args) {
		int result = 0;
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
			download(date);
		} catch (Throwable t) {
			logger.severe("Throwable: " + t.getMessage());
			result = 1;
		} finally {
			System.exit(result);
		}
	}

}
