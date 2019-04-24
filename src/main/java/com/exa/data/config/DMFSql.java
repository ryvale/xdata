package com.exa.data.config;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.exa.data.DataReader;
import com.exa.data.sql.SQLDataReader;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;

import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;


public class DMFSql extends DataManFactory {

	
	private Map<String, DataSource> dataSources = new HashMap<>();
	
	private String defaultDataSource;
	
	public DMFSql(FilesRepositories filesRepos, Map<String, DataSource> dataSources, String defaultDataSource) {
		super(filesRepos);
		this.dataSources = dataSources;
		this.defaultDataSource = defaultDataSource;
	}

	@Override
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext variableContext) throws ManagedException {
		
		String dsName = ovEntity.getAttributAsString("dataSource");
		
		if(dsName == null) dsName = defaultDataSource;
		
		if(dsName == null) throw new ManagedException(String.format("No data source provided."));
		
		DataSource ds = dataSources.get(dsName);
		if(ds == null) throw new ManagedException(String.format("The data source %s specified  is not present.", dsName));
		
		DataReader<?> dr = new SQLDataReader(name, ds, eval, variableContext, ovEntity);
		
		return dr;
	}

}
