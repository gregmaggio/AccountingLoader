/**
 * 
 */
package ca.datamagic.accounting.batch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ca.datamagic.accounting.dao.BigQueryDAO;
import ca.datamagic.accounting.dao.StorageDAO;

/**
 * @author gregm
 *
 */
public class BatchLoader {
	private static final Logger logger = LogManager.getLogger(BatchLoader.class);
	
	/**
	 * Load an AVRO file for a date
	 * @param args
	 */
	public static void main(String[] args) {
		StorageDAO storageDAO = new StorageDAO();
		BigQueryDAO bigQueryDAO = new BigQueryDAO();
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
			if (date == null) {
				System.out.println("Usage: ca.datamagic.accounting.batch.BatchLoader --date \"yyyy-MM-dd\"");
				return;
			}
			String blob = storageDAO.read(date);
			logger.debug("blob: " + blob);
			String avroFile = blob.substring(blob.lastIndexOf('/') + 1);
			logger.debug("avroFile: " + avroFile);						
			long events = bigQueryDAO.getEvents(avroFile);
			logger.debug("events: " + events);
			if (events > 0L) {
				bigQueryDAO.deleteEvents(avroFile);
			}
			bigQueryDAO.loadAvro(blob);			
		} catch (Throwable t) {
			logger.error("Throwable", t);
		}
	}

}
