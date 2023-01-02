/**
 * 
 */
package ca.datamagic.accounting.task;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Logger;

import ca.datamagic.accounting.batch.BatchConverter;
import ca.datamagic.accounting.batch.BatchDownloader;
import ca.datamagic.accounting.batch.BatchLoader;
import ca.datamagic.accounting.batch.BatchUploader;
import ca.datamagic.accounting.dao.FileDAO;
import ca.datamagic.accounting.dto.DailyTaskDTO;

/**
 * @author gregm
 *
 */
public class DailyTask implements Runnable {
	private static final Logger logger = Logger.getLogger(DailyTask.class.getName());
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static final List<DailyTask> dailyTasks = new ArrayList<>();
	private String date = null;
	private boolean running = false;
	private boolean error = false;
	private boolean cleanUp = false;
	
	public DailyTask(String date) {
		this.date = date;
		this.cleanUp = false;
		synchronized (dailyTasks) {
			dailyTasks.add(this);
		}
	}
	
	public DailyTask(String date, boolean cleanUp) {
		this.date = date;
		this.cleanUp = cleanUp;
		synchronized (dailyTasks) {
			dailyTasks.add(this);
		}
	}
	
	public static DailyTaskDTO[] getDailyTasks() {
		synchronized (dailyTasks) {
			DailyTaskDTO[] tasks = new DailyTaskDTO[dailyTasks.size()];
			for (int ii = 0; ii < tasks.length; ii++) {
				DailyTask task = dailyTasks.get(ii);
				tasks[ii] = new DailyTaskDTO(task.getDate(), task.isRunning(), task.isError(), task.isCleanUp());
			}
			return tasks;
		}
	}
	
	public String getDate() {
		return this.date;
	}
	
	public boolean isRunning() {
		return this.running;
	}
	
	public boolean isError() {
		return this.error;
	}
	
	public boolean isCleanUp() {
		return this.cleanUp;
	}
	
	public void run() {
		this.running = true;
		try {
			if (this.cleanUp) {
				clean(this.date);
			} else {
				daily(this.date);
			}
		} catch (IOException ex) {
			logger.severe("IOException: " + ex.getMessage());
			this.error = true;
		} catch (InterruptedException ex) {
			logger.severe("InterruptedException: " + ex.getMessage());
			this.error = true;
		}
		this.running = false;
	}
	
	public static void daily(String date) throws IOException, InterruptedException {
		logger.info("date: " + date);
		BatchDownloader.download(date);
		BatchConverter.convert(date);
		BatchUploader.upload(date);
		BatchLoader.load(date);
	}
	
	public static void clean(String date) throws IOException {
		logger.info("date: " + date);
		FileDAO dao = new FileDAO();
		if ((date != null) && (date.length() > 0) && (date.compareToIgnoreCase("null") != 0)) {
			dao.deleteCSVFile(date);
			dao.deleteAVROFile(date);
		} else {
			dao.deleteCSVFiles();
			dao.deleteAVROFiles();
		}
	}
	
	public static void executeDaily(String date) {
	     new Thread(new DailyTask(date)).start();
	}
	
	public static void executeClean(String date) {
		new Thread(new DailyTask(date, true)).start();
	}
	
	/**
	 * Run the daily task to download, process, upload, and load a log file.
	 */
	public static void main(String[] args) {
		int result = 0;
		try {
			logger.info("DailyTask");
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.DATE, -1);
			String date = dateFormat.format(calendar.getTime());
			daily(date);
		} catch (Throwable t) {
			logger.severe("Throwable: " + t.getMessage());
			result = 1;
		} finally {
			System.exit(result);
		}
	}
}
