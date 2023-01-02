/**
 * 
 */
package ca.datamagic.accounting.dto;

/**
 * @author gregm
 *
 */
public class DailyTaskDTO {
	private String date = null;
	private boolean running = false;
	private boolean error = false;
	private boolean cleanUp = false;
	
	public DailyTaskDTO() {
		
	}
	
	public DailyTaskDTO(String date, boolean running, boolean error, boolean cleanUp) {
		this.date = date;
		this.running = running;
		this.error = error;
		this.cleanUp = cleanUp;
	}

	public String getDate() {
		return this.date;
	}
	public void setDate(String newVal) {
		this.date = newVal;
	}
	public boolean isRunning() {
		return this.running;
	}
	public void setRunning(boolean newVal) {
		this.running = newVal;
	}
	public boolean isError() {
		return this.error;
	}
	public void setError(boolean newVal) {
		this.error = newVal;
	}
	public boolean isCleanUp() {
		return this.cleanUp;
	}
	public void setCleanUp(boolean newVal) {
		this.cleanUp = newVal;
	}
}
