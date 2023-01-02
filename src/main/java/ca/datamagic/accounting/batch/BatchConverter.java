/**
 * 
 */
package ca.datamagic.accounting.batch;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Logger;

import ca.datamagic.accounting.avro.Converter;
import ca.datamagic.accounting.dao.FileDAO;

/**
 * @author gregm
 *
 */
public class BatchConverter {
	private static final Logger logger = Logger.getLogger(BatchConverter.class.getName());
	
	public static void convert(String date) throws IOException {
		logger.info("date: " + date);
		FileDAO dao = new FileDAO();
		try {
			String csvDirectory = dao.getCSVDirectory();
			logger.info("csvDirectory: " + csvDirectory);
			
			String avroDirectory = dao.getAVRODirectory();
			logger.info("avroDirectory: " + avroDirectory);
			
			String csvFileName = MessageFormat.format("{0}/accounting.csv.{1}", csvDirectory, date);
			logger.info("csvFileName: " + csvFileName);
			
			String avroFileName = MessageFormat.format("{0}/accounting.avro.{1}", avroDirectory, date);
			logger.info("avroFileName: " + avroFileName);
			
			Converter converter = new Converter();
			converter.setCsvFileName(csvFileName);
			converter.setAvroFileName(avroFileName);
			converter.convert();
		} finally {
			dao.disconnect();
		}
	}
	
	/**
	 * Convert to AVRO a file for a date or all input files
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
			convert(date);
		} catch (Throwable t) {
			logger.severe("Throwable: " + t.getMessage());
			result = 1;
		} finally {
			System.exit(result);
		}
	}

}
