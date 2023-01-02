/**
 * 
 */
package ca.datamagic.accounting.servlet;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;

/**
 * @author gregm
 *
 */
public class SwaggerAPIServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		URI uri = null;
		try {
			uri = new URI(request.getRequestURL().toString());
		} catch (Throwable t) {
			uri = null;
		}
		StringBuffer host = new StringBuffer();
		if (uri != null) {
			host.append(uri.getHost());
			if ((uri.getPort() != -1) && (uri.getPort() != 80) && (uri.getPort() != 443)) {
				host.append(":");
				host.append(Integer.toString(uri.getPort()));
			}
		}		
		InputStream input = getServletContext().getResourceAsStream("/WEB-INF/swagger.json");
		String theString = IOUtils.toString(input, "UTF-8").replace("{{host}}", host.toString()); 
		input.close();
		response.setContentType("application/json");
		response.getWriter().println(theString);
	}
}
