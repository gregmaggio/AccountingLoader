/**
 * 
 */
package ca.datamagic.accounting.servlet;

import java.io.File;
import java.text.MessageFormat;
import java.util.logging.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import ca.datamagic.accounting.dao.FileDAO;

/**
 * @author gregm
 *
 */
public class AccountingContextListener implements ServletContextListener {
	private static final Logger logger = Logger.getLogger(AccountingContextListener.class.getName());
	
	@Override
	public void contextInitialized(ServletContextEvent sce) {
		logger.info("contextInitialized");
		String tempPath = "/tmp";
		logger.info("tempPath: " + tempPath);
		File tempFile = new File(tempPath);
		if (!tempFile.exists()) {
			tempFile.mkdir();
		}
		String csvPath = MessageFormat.format("{0}/csv", tempPath);
		logger.info("csvPath: " + csvPath);
		File csvFile = new File(csvPath);
		if (!csvFile.exists()) {
			csvFile.mkdir();
		}
		String avroPath = MessageFormat.format("{0}/avro", tempPath);
		logger.info("avroPath: " + avroPath);
		File avroFile = new File(avroPath);
		if (!avroFile.exists()) {
			avroFile.mkdir();
		}
		FileDAO.setTempPath(tempPath);
		
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		logger.info("contextDestroyed");
	}
}
