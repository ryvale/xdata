package com.exa.data.web;

import java.io.IOException;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;


import com.exa.data.DataException;
import com.exa.data.DynamicField;
import com.exa.data.config.utils.DataUserException;
import com.exa.utils.ManagedException;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

public class RMJSONObject extends ResponseManager {
	
	private JSONObject valuesCache = null;
	
	private Map<String, DynamicField> fields;
	
	private boolean read = false;
	
	private String path;

	public RMJSONObject(Map<String, DynamicField> fields) throws IOException {
		this.fields = fields;
	}
	
	public RMJSONObject(Map<String, DynamicField> fields, String path) {
		super();
		this.fields = fields;
		this.path = path;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	@Override
	public boolean next() {
		if(read) return false;
		
		read = true;
		return true;
	}

	@Override
	public void manage(Response response) throws DataException {
		String strResp;
		try {
			strResp = response.body().string();
			Integer httpStatus = response.code();
			if(WSDataWriter.debugOn || WSDataReader.debugOn) System.out.println("http status : " +  httpStatus + "\n responseBody :" + strResp);
			if(httpStatus != 200) throw new DataUserException(strResp, "WEB_SERVICE_ERROR - http status : " + httpStatus);
			
			valuesCache = new JSONObject(strResp);
			
			if(path == null) return;
			
			String[] parts = path.split("[.]");
			
			JSONObject mpValues = valuesCache;
			
			for(int i=0; i < parts.length;i++) {
				JSONObject obj = mpValues.getJSONObject(parts[i]);
				if(obj == null) read = true;

				mpValues = obj;
			}
			
			valuesCache = mpValues;
		} catch (IOException | JSONException e) {
			e.printStackTrace();
			throw new DataException(e);
		}
	}

	@Override
	public RMJSONObject asRMJSONObject() {
		return this;
	}
	
	//@SuppressWarnings("unchecked")
	private Object browseCache(String path) throws JSONException  {
		String[] parts = path.split("[.]");
		
		JSONObject mpValues = valuesCache;
		
		for(int i=0; i < parts.length-1; i++) {
			JSONObject obj = mpValues.getJSONObject(parts[i]);
			if(obj == null) read = true;

			mpValues = obj;
		}
		
		return mpValues.get(parts[parts.length - 1]);
	}
	
	@Override
	public String getString(String name) throws DataException {
		DynamicField df = fields.get(name);
		
		if(df == null) throw new DataException(String.format("The field %s is not defined", name));
		
		if(!"string".equals(df.getType())) throw new DataException(String.format("The field %s is not string", name));
		
		try {
			if("ws".equals(df.getExpType()))  {
				String path = df.getVlExp().asString();
				
				Object ob = browseCache(path);
				if(!(ob instanceof String))  throw new ManagedException(String.format("The field %s value is not string", name));
				
				return ob.toString();
			}
			
			if("value".equals(df.getExpType())) return df.getVlExp().asString();
		
		return null;
		
		}
		catch (Exception e) {
			throw new DataException(e);
		}
	}

	@Override
	public void manage(Request.Builder builder) throws IOException, ManagedException {
		builder.addHeader("content-type", "application/json");
		
	}
	

}
