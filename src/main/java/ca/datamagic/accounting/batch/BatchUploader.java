/**
 * 
 */
package ca.datamagic.accounting.batch;

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ca.datamagic.accounting.dao.FileDAO;
import ca.datamagic.accounting.dao.StorageDAO;

/**
 * @author gregm
 *
 */
public class BatchUploader {
	private static final Logger logger = LogManager.getLogger(BatchUploader.class);
	
	/**
	 * Upload an AVRO file for a date or all AVRO files
	 * @param args
	 */
	public static void main(String[] args) {
		FileDAO fileDAO = new FileDAO();
		StorageDAO storageDAO = new StorageDAO();
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
			
			String avroDirectory = fileDAO.getAVRODirectory();
			String[] avroFiles = fileDAO.getAVROFiles();
			for (int ii = 0; ii < avroFiles.length; ii++) {
				String avroFile = avroFiles[ii];
				logger.debug("avroFile: " + avroFile);
				String avroFileName = MessageFormat.format("{0}/{1}", avroDirectory, avroFile);
				logger.debug("avroFileName: " + avroFileName);				
				if (date != null) {
					if (avroFile.contains(date)) {
						storageDAO.upload(avroFileName);
					}
					continue;
				}
				storageDAO.upload(avroFileName);
			}
		} catch (Throwable t) {
			logger.error("Throwable", t);
		}
		try {
			fileDAO.disconnect();
		} catch (IOException ex) {
			logger.warn("IOException", ex);
		}
	}

}
