package com.exa.data.ws;

import java.io.IOException;
import java.util.Map;

import org.springframework.boot.json.JsonJsonParser;

import com.exa.data.DynamicField;
import com.exa.utils.ManagedException;
import com.squareup.okhttp.Response;

public class RMJSONObject extends ResponseManager {
	//protected Response response;
	JsonJsonParser jsonParser = new JsonJsonParser();
	
	private Map<String, Object> valuesCache = null;
	
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

	
	@SuppressWarnings("unchecked")
	@Override
	public void open(Response response) throws IOException, ManagedException {
		Map<String, Object> valuesCacheRoot = jsonParser.parseMap(response.body().string());
		if(path == null) {
			valuesCache = valuesCacheRoot;
			return;
		}
		
		String[] parts = path.split("[.]");
		
		Map<String, Object> mpValues = valuesCache;
		
		for(int i=0; i < parts.length;i++) {
			Object obj = mpValues.get(parts[i]);
			if(obj == null) read = true;
			
			if(!(obj instanceof Map)) throw new ManagedException(String.format("Non json object met when following the path %", path));

			mpValues = (Map<String, Object>)obj;
		}
		
		valuesCache = mpValues;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public RMJSONObject asRMJSONObject() {
		return this;
	}
	
	@SuppressWarnings("unchecked")
	private Object browseCache(String path) throws ManagedException {
		String[] parts = path.split("[.]");
		
		Map<String, Object> mpValues = valuesCache;
		
		for(int i=0; i < parts.length-1; i++) {
			Object obj = mpValues.get(parts[i]);
			if(obj == null) read = true;
			
			if(!(obj instanceof Map)) throw new ManagedException(String.format("Non json object met when following the path %", path));

			mpValues = (Map<String, Object>)obj;
		}
		
		return mpValues.get(parts[parts.length - 1]);
	}
	
	@Override
	public String getString(String name) throws ManagedException {
		DynamicField df = fields.get(name);
		
		if(df == null) throw new ManagedException(String.format("The field %s is not defined", name));
		
		if(!"string".equals(df.getType())) throw new ManagedException(String.format("The field %s is not string", name));
		
		if("ws".equals(df.getExpType()))  {
			String path = df.getVlExp().asString();
			
			Object ob = browseCache(path);
			if(!(ob instanceof String))  throw new ManagedException(String.format("The field %s value is not string", name));
			
			return ob.toString();
		}
		
		if("value".equals(df.getExpType())) return df.getVlExp().asString();
		
		return null;
	}
	

}
