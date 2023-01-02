/**
 * 
 */
package ca.datamagic.accounting.batch;

import java.io.IOException;
import java.util.logging.Logger;

import ca.datamagic.accounting.dao.BigQueryDAO;
import ca.datamagic.accounting.dao.StorageDAO;

/**
 * @author gregm
 *
 */
public class BatchLoader {
	private static final Logger logger = Logger.getLogger(BatchLoader.class.getName());
	
	public static void load(String date) throws IOException, InterruptedException {
		logger.info("date: " + date);
		StorageDAO storageDAO = new StorageDAO();
		BigQueryDAO bigQueryDAO = new BigQueryDAO();
		String blob = storageDAO.read(date);
		logger.info("blob: " + blob);
		String avroFile = blob.substring(blob.lastIndexOf('/') + 1);
		logger.info("avroFile: " + avroFile);						
		long events = bigQueryDAO.getEvents(avroFile);
		logger.info("events: " + events);
		if (events > 0L) {
			bigQueryDAO.deleteEvents(avroFile);
		}
		bigQueryDAO.loadAvro(blob);	
	}
	
	/**
	 * Load an AVRO file for a date
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
			load(date);
		} catch (Throwable t) {
			logger.severe("Throwable: " + t.getMessage());
			result = 1;
		} finally {
			System.exit(result);
		}
	}

}
