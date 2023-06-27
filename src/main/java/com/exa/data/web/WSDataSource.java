package com.exa.data.web;

import com.exa.data.MapDataSource;
import com.exa.data.XADataSource;
import com.exa.data.config.utils.DataUserException;
import com.exa.data.sql.XASQLDataSource;

import okhttp3.Request;

public class WSDataSource implements XADataSource {
	
	private String protocol = null;
	
	private String server;
	
	private String port;
	
	private String urlPrefix;
	
	private String tokenMethod = null;
	
	private String token = null;
	
	private String tokenName = null;
	
	
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

	public Request.Builder getRequestBuilder(String resource) throws DataUserException {
		String url = (protocol + "://" + server + (port == null ? "" : ":" + port) + withFirstSlash(urlPrefix)) + withFirstSlash(resource);
	
		Request.Builder res = new Request.Builder().url(url);
		
		if(token != null || tokenMethod != null || tokenName != null) {
			if(token == null || tokenMethod == null || tokenName == null) throw new DataUserException(String.format("The web service (%s) token is misconfiguated", url), "WEB_SERVICE_TOKEN_MISCONFIG");
			
			if(!tokenMethod.equals("Header")) throw new DataUserException(String.format("The web service (%s) token is misconfiguated. '%s' : Unknow token method", url, tokenMethod), "WEB_SERVICE_TOKEN_MISCONFIG");
			
			res.addHeader(tokenName, token);
		}
		
		return res;
	}
	
	static String withFirstSlash(String urlPart) {
		return urlPart == null ? "" : (urlPart.startsWith("/") ? urlPart : "/" + urlPart);
	}

	@Override
	public MapDataSource asMapDataSource() {
		return null;
	}

	public String getTokenMethod() {
		return tokenMethod;
	}

	public void setTokenMethod(String tokenMethod) {
		this.tokenMethod = tokenMethod;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public String getTokenName() {
		return tokenName;
	}

	public void setTokenName(String tokenName) {
		this.tokenName = tokenName;
	}
	
	

}
