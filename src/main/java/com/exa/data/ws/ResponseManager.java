package com.exa.data.ws;

import java.io.IOException;

import com.exa.utils.ManagedException;
import com.squareup.okhttp.Response;

public abstract class ResponseManager {
	/*protected Response response;
	
	public ResponseManager(Response response) {
		super();
		this.response = response;
	}*/
	
	public abstract void open(Response respone) throws IOException, ManagedException;
	
	public abstract void close();

	public abstract boolean next();
	
	public abstract String getString(String name) throws ManagedException;
	
	public abstract RMJSONObject asRMJSONObject();
 
}
