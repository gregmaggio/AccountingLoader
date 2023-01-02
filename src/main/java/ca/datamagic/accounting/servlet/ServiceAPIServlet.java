/**
 * 
 */
package ca.datamagic.accounting.servlet;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;

import ca.datamagic.accounting.dao.FileDAO;
import ca.datamagic.accounting.dto.DailyTaskDTO;
import ca.datamagic.accounting.task.DailyTask;

/**
 * @author gregm
 *
 */
public class ServiceAPIServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(ServiceAPIServlet.class.getName());
	private static final Pattern importersPattern = Pattern.compile("/importers", Pattern.CASE_INSENSITIVE);
	private static final Pattern csvFilesPattern = Pattern.compile("/csvFiles", Pattern.CASE_INSENSITIVE);
	private static final Pattern avroFilesPattern = Pattern.compile("/avroFiles", Pattern.CASE_INSENSITIVE);
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			String origin = request.getHeader("Origin");
			logger.info("origin: " + origin);
			String pathInfo = request.getPathInfo();
			logger.info("pathInfo: " + pathInfo);
			Matcher importersMatcher = importersPattern.matcher(pathInfo);
			if (importersMatcher.matches()) {
				logger.info("importers");
				DailyTaskDTO[] tasks = DailyTask.getDailyTasks();
				Gson gson = new Gson();
				String json = gson.toJson(tasks);
				response.setContentType("application/json");
				response.getWriter().println(json);
				return;
			}
			Matcher csvFilesMatcher = csvFilesPattern.matcher(pathInfo);
			if (csvFilesMatcher.matches()) {
				logger.info("csvFiles");
				FileDAO dao = new FileDAO();
				String[] csvFiles = dao.getCSVFiles();
				Gson gson = new Gson();
				String json = gson.toJson(csvFiles);
				response.setContentType("application/json");
				response.getWriter().println(json);
				return;
			}
			Matcher avroFilesMatcher = avroFilesPattern.matcher(pathInfo);
			if (avroFilesMatcher.matches()) {
				logger.info("avroFiles");
				FileDAO dao = new FileDAO();
				String[] avroFiles = dao.getAVROFiles();
				Gson gson = new Gson();
				String json = gson.toJson(avroFiles);
				response.setContentType("application/json");
				response.getWriter().println(json);
				return;
			}
			response.sendError(403);
		} catch (Throwable t) {
			logger.log(Level.SEVERE, "Throwable", t);
			throw new IOException("Exception", t);
		}
	}
}
