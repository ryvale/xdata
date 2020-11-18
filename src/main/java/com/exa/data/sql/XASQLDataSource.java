package com.exa.data.sql;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.exa.data.MapDataSource;
import com.exa.data.XADataSource;
import com.exa.data.web.WSDataSource;

public class XASQLDataSource implements XADataSource {
	
	private DataSource dataSource;
	
	private Connection sharedConnection;
	
	private int refCount = 0;
	
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
	
	public Connection getNewConnection() throws SQLException {
		Connection res = dataSource.getConnection();
		if(SQLDataReader.debugOn || SQLDataWriter.debugOn) System.out.println("Connection open id-" + res.hashCode());
		return res;
	}
	
	public Connection getSharedConnection() throws SQLException {
		if(refCount++ == 0) {
			sharedConnection = dataSource.getConnection();
			if(SQLDataReader.debugOn || SQLDataWriter.debugOn) System.out.println("Connection open id-" + sharedConnection.hashCode());
		}
		return sharedConnection;
	}
	
	public void releaseSharedConnection() {
		if(refCount > 0) refCount--;
		if(refCount == 0){
			try {
				sharedConnection.close();
				if(SQLDataReader.debugOn || SQLDataWriter.debugOn) System.out.println("Connection closed id-" + sharedConnection.hashCode());
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public MapDataSource asMapDataSource() {
		return null;
	}

}
