package com.exa.data.web;

import java.io.IOException;
import java.util.Map;

import org.json.JSONArray;

import com.exa.data.DynamicField;
import com.exa.utils.ManagedException;
import com.squareup.okhttp.Request.Builder;
import com.squareup.okhttp.Response;

public class RMJSONArray extends ResponseManager {
	
	private JSONArray valuesCache = null;
	
	private int index = 0;
	
	private Map<String, DynamicField> fields;
	
	private String path;

	public RMJSONArray(Map<String, DynamicField> fields) throws IOException {
		this.fields = fields;
	}
	
	public RMJSONArray(Map<String, DynamicField> fields, String path) {
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
	public void manage(Builder builder) throws IOException, ManagedException {
		
	}

	@Override
	public void manage(Response respone) throws IOException, ManagedException {

		
	}

	@Override
	public boolean next() {

		return false;
	}

	@Override
	public String getString(String name) throws ManagedException {
	
		return null;
	}

	@Override
	public RMJSONObject asRMJSONObject() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return false;
	}

}
