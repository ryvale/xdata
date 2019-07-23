package com.exa.data.web;

import com.exa.data.MapDataSource;
import com.exa.data.XADataSource;
import com.exa.data.sql.XASQLDataSource;

import com.squareup.okhttp.Request;

public class WSDataSource implements XADataSource {
	
	private String protocol = null;
	
	private String server;
	
	private String port;
	
	private String urlPrefix;
	
	
	public WSDataSource() {
		super();
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
	
	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public Request.Builder getRequestBuilder(String resource) {
		String url = (protocol + "://" + server + (port == null ? "" : ":" + port) + withFirstSlash(urlPrefix)) + withFirstSlash(resource);
		
		return new Request.Builder().url(url);
	}
	
	static String withFirstSlash(String urlPart) {
		return urlPart == null ? "" : (urlPart.startsWith("/") ? urlPart : "/" + urlPart);
	}

	@Override
	public MapDataSource asMapDataSource() {
		// TODO Auto-generated method stub
		return null;
	}

}
