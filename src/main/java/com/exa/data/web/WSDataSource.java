package com.exa.data.web;

import com.exa.data.XADataSource;
import com.exa.data.sql.XASQLDataSource;

import com.squareup.okhttp.Request;

public class WSDataSource implements XADataSource {
	
	private String urlPrefix = null;
	
	public WSDataSource() {
		super();
	}

	public WSDataSource(String urlPrefix) {
		super();
		this.urlPrefix = urlPrefix;
	}

	
	@Override
	public XASQLDataSource asXASQLDataSource() {
		return null;
	}

	@Override
	public WSDataSource asXAWSDataSource() {
		return this;
	}

	public String getUrlPrefix() {
		return urlPrefix;
	}

	public void setUrlPrefix(String urlPrefix) {
		this.urlPrefix = urlPrefix;
	}
	
	public Request.Builder getRequestBuilder(String resource) {
		String url = (urlPrefix == null ? "" :  urlPrefix) + (resource == null ? "" : resource);
		
		return new Request.Builder().url(url);
	}

}
