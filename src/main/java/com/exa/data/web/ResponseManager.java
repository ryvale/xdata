package com.exa.data.web;

import java.io.IOException;

import com.exa.utils.ManagedException;
import okhttp3.Request;
import okhttp3.Response;

public abstract class ResponseManager {
	
	public abstract void manage(Request.Builder builder) throws IOException, ManagedException;
	
	public abstract void manage(Response respone) throws IOException, ManagedException;

	public abstract boolean next();
	
	public abstract String getString(String name) throws ManagedException;
	
	public abstract RMJSONObject asRMJSONObject();
	
	public abstract boolean isOpen();
 
}
