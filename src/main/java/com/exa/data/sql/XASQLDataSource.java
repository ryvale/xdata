package com.exa.data.sql;

import javax.sql.DataSource;

import com.exa.data.MapDataSource;
import com.exa.data.XADataSource;
import com.exa.data.web.WSDataSource;

public class XASQLDataSource implements XADataSource {
	
	private DataSource dataSource;
	
	public XASQLDataSource(DataSource dataSource) {
		super();
		this.dataSource = dataSource;
	}

	@Override
	public XASQLDataSource asXASQLDataSource() {
		return this;
	}

	@Override
	public WSDataSource asXAWSDataSource() {
		return null;
	}
	
	public DataSource getDataSource() { return dataSource; }

	@Override
	public MapDataSource asMapDataSource() {
		// TODO Auto-generated method stub
		return null;
	}

}
