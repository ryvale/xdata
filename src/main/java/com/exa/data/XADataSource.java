package com.exa.data;

import com.exa.data.sql.XASQLDataSource;
import com.exa.data.ws.WSDataSource;

public interface XADataSource {

	XASQLDataSource asXASQLDataSource();
	
	WSDataSource asXAWSDataSource();
}
