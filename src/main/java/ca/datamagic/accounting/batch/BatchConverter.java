/**
 * 
 */
package ca.datamagic.accounting.batch;

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ca.datamagic.accounting.avro.Converter;
import ca.datamagic.accounting.dao.FileDAO;

/**
 * @author gregm
 *
 */
public class BatchConverter {
	private static final Logger logger = LogManager.getLogger(BatchConverter.class);
	
	/**
	 * Convert to AVRO a file for a date or all input files
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
			
			String csvDirectory = dao.getCSVDirectory();
			String avroDirectory = dao.getAVRODirectory();
			String[] csvFiles = dao.getCSVFiles();
			for (int ii = 0; ii < csvFiles.length; ii++) {
				String csvFile = csvFiles[ii];
				logger.debug("csvFile: " + csvFile);
				String csvFileName = MessageFormat.format("{0}/{1}", csvDirectory, csvFile);
				logger.debug("csvFileName: " + csvFileName);
				String avroFileName = MessageFormat.format("{0}/{1}", avroDirectory, csvFile.replace(".csv", ".avro"));
				logger.debug("avroFileName: " + avroFileName);
				if (date != null) {
					if (csvFile.contains(date)) {
						Converter converter = new Converter();
						converter.setCsvFileName(csvFileName);
						converter.setAvroFileName(avroFileName);
						converter.convert();
					}
					continue;
				}				
				Converter converter = new Converter();
				converter.setCsvFileName(csvFileName);
				converter.setAvroFileName(avroFileName);
				converter.convert();
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
