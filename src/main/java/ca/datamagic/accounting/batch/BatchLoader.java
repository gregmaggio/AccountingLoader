/**
 * 
 */
package ca.datamagic.accounting.batch;

import java.text.MessageFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	private static final Pattern datePattern = Pattern.compile("(?<year>\\d+)-(?<month>\\d+)-(?<day>\\d+)", Pattern.CASE_INSENSITIVE);
	
	/**
	 * Load an AVRO file for a date
	 * @param args
	 */
	public static void main(String[] args) {
		StorageDAO storageDAO = new StorageDAO();
		BigQueryDAO bigQueryDAO = new BigQueryDAO();
		try {
			String date = "2022-02-04";
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
			String bucketName = storageDAO.getBucketName();
			String[] blobs = storageDAO.list();
			for (int ii = 0; ii < blobs.length; ii++) {
				String blob = blobs[ii];
				logger.debug("blob: " + blob);
				Matcher dateMatcher = datePattern.matcher(blob);
				if (!dateMatcher.find()) {
					logger.warn("Blob not correct format.");
					continue;					
				}
				int year = Integer.parseInt(dateMatcher.group("year"));
				int month = Integer.parseInt(dateMatcher.group("month"));
				int day = Integer.parseInt(dateMatcher.group("day"));								
				if (blob.contains(date)) {
					long events = bigQueryDAO.getEvents(year, month, day);
					logger.debug("events: " + events);
					if (events > 0L) {
						bigQueryDAO.deleteEvents(year, month, day);
					}
					String sourceUri = MessageFormat.format("gs://{0}/{1}", bucketName, blob);
					logger.debug("sourceUri: " + sourceUri);
					bigQueryDAO.loadAvro(sourceUri);
					break;
				}
			}
		} catch (Throwable t) {
			logger.error("Throwable", t);
		}
	}

}
