/**
 * 
 */
package ca.datamagic.accounting.dto;

/**
 * @author gregm
 *
 */
public class SwaggerResourceDTO {
	private String name = "default";
	private String location = "/v2/api-docs";
	private String swaggerVersion = "2.0";
	
	public String getName() {
		return this.name;
	}
	
	public String getLocation() {
		return this.location;
	}
	
	public String getSwaggerVersion() {
		return this.swaggerVersion;
	}
	
	public void setName(String newVal) {
		this.name = newVal;
	}
	
	public void setLocation(String newVal) {
		this.location = newVal;
	}
	
	public void setSwaggerVersion(String newVal) {
		this.swaggerVersion = newVal;
	}
}
