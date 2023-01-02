/**
 * 
 */
package ca.datamagic.accounting.servlet;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import ca.datamagic.accounting.task.DailyTask;

/**
 * @author gregm
 *
 */
public class CronAPIServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(CronAPIServlet.class.getName());
	private static final Pattern importPattern = Pattern.compile("/import(/(?<date>\\d+-\\d+-\\d+))?", Pattern.CASE_INSENSITIVE);
	private static final Pattern cleanPattern = Pattern.compile("/clean(/(?<date>\\d+-\\d+-\\d+))?", Pattern.CASE_INSENSITIVE);
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			String origin = request.getHeader("Origin");
			logger.info("origin: " + origin);
			String pathInfo = request.getPathInfo();
			logger.info("pathInfo: " + pathInfo);			
			Matcher importMatcher = importPattern.matcher(pathInfo);
			if (importMatcher.matches()) {
				logger.info("import");
				String date = importMatcher.group("date");
				if ((date == null) || (date.length() < 1)) {
					Calendar calendar = Calendar.getInstance();
					calendar.add(Calendar.DATE, -1);
					date = dateFormat.format(calendar.getTime());
				}
				logger.info("date: " + date);
				DailyTask.executeDaily(date);
				response.setStatus(204);
				return;
			}
			Matcher cleanMatcher = cleanPattern.matcher(pathInfo);
			if (cleanMatcher.matches()) {
				logger.info("clean");
				String date = cleanMatcher.group("date");
				logger.info("date: " + date);
				DailyTask.executeClean(date);
				response.setStatus(204);
				return;
			}
			response.sendError(403);
		} catch (Throwable t) {
			logger.log(Level.SEVERE, "Throwable", t);
			throw new IOException("Exception", t);
		}
	}
}
