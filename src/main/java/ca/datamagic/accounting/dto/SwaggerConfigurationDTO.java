/**
 * 
 */
package ca.datamagic.accounting.dto;

/**
 * @author gregm
 *
 */
public class SwaggerConfigurationDTO {
	private String validatorUrl = "validatorUrl";
	private String docExpansion = "none";
	private String apisSorter = "alpha";
	private String defaultModelRendering = "schema";
	private String[] supportedSubmitMethods = new String[] { "get", "post", "put", "delete", "patch" };
	private boolean jsonEditor = false;
	private boolean showRequestHeaders = true;
	
	public String getValidatorUrl() {
		return this.validatorUrl;
	}
	
	public String getDocExpansion() {
		return this.docExpansion;
	}
	
	public String getApisSorter() {
		return this.apisSorter;
	}
	
	public String getDefaultModelRendering() {
		return this.defaultModelRendering;
	}
	
	public String[] getSupportedSubmitMethods() {
		return this.supportedSubmitMethods;
	}
	
	public boolean isJsonEditor() {
		return this.jsonEditor;
	}
	
	public boolean isShowRequestHeaders() {
		return this.showRequestHeaders;
	}
	
	public void setValidatorUrl(String newVal) {
		this.validatorUrl = newVal;
	}
	
	public void setDocExpansion(String newVal) {
		this.docExpansion = newVal;
	}
	
	public void setApisSorter(String newVal) {
		this.apisSorter = newVal;
	}
	
	public void setDefaultModelRendering(String newVal) {
		this.defaultModelRendering = newVal;
	}
	
	public void setSupportedSubmitMethods(String[] newVal) {
		this.supportedSubmitMethods = newVal;
	}
	
	public void setJsonEditor(boolean newVal) {
		this.jsonEditor = newVal;
	}
	
	public void setShowRequestHeaders(boolean newVal) {
		this.showRequestHeaders = newVal;
	}
}
