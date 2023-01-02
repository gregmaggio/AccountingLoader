/**
 * 
 */
package ca.datamagic.accounting.batch;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Logger;

import ca.datamagic.accounting.dao.FileDAO;
import ca.datamagic.accounting.dao.StorageDAO;

/**
 * @author gregm
 *
 */
public class BatchUploader {
	private static final Logger logger = Logger.getLogger(BatchUploader.class.getName());
	
	public static void upload(String date) throws IOException {
		logger.info("date: " + date);
		FileDAO fileDAO = new FileDAO();
		StorageDAO storageDAO = new StorageDAO();
		try {
			String avroDirectory = fileDAO.getAVRODirectory();
			logger.info("avroDirectory: " + avroDirectory);
			
			String avroFileName = MessageFormat.format("{0}/accounting.avro.{1}", avroDirectory, date);
			logger.info("avroFileName: " + avroFileName);
			
			storageDAO.upload(avroFileName);
		} finally {
			try {
				fileDAO.disconnect();
			} catch (IOException ex) {
				logger.warning("IOException: " + ex.getMessage());
			}
		}
	}
	/**
	 * Upload an AVRO file for a date or all AVRO files
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
			upload(date);
		} catch (Throwable t) {
			logger.severe("Throwable: " + t.getMessage());
			result = 1;
		} finally {
			System.exit(result);
		}
	}

}
