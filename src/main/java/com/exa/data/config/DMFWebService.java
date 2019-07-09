package com.exa.data.config;

import java.util.HashMap;
import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.XADataSource;
import com.exa.data.web.WSDataReader;
import com.exa.data.web.WSDataSource;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.expression.parsing.Parser.UnknownIdentifierValidation;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public class DMFWebService extends DataManFactory {
	
	private Map<String, XADataSource> dataSources = new HashMap<>();
	
	private String defaultDataSource;
	
	public DMFWebService(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource) {
		super(filesRepos);
		this.dataSources = dataSources;
		this.defaultDataSource = defaultDataSource;
	}
	
	public DMFWebService(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, UnknownIdentifierValidation uiv) {
		super(filesRepos, uiv);
		this.dataSources = dataSources;
		this.defaultDataSource = defaultDataSource;
	}

	@Override
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval,VariableContext vc) throws ManagedException {
		String dsName = ovEntity.getAttributAsString("dataSource");
		
		if(dsName == null) dsName = defaultDataSource;
		
		if(dsName == null) throw new ManagedException(String.format("No data source provided."));
		
		XADataSource xaDS = dataSources.get(dsName);
		if(xaDS == null) throw new ManagedException(String.format("The data source %s specified is not present.", dsName));
		
		WSDataSource ds = xaDS.asXAWSDataSource();
		if(ds == null) throw new ManagedException(String.format("The data source %s specified should be ws type.", dsName));
		
		/*DataSource ds = xasqlds.getDataSource();
		if(ds == null) throw new ManagedException(String.format("The data source %s specified is not present.", dsName));*/
		
		DataReader<?> dr = new WSDataReader(dsName, eval, vc, ovEntity, ds);
		
		return dr;
	}

	@Override
	public DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext vc, DataReader<?> drSource, boolean preventInsertion, boolean preventUpdate)
			throws ManagedException {
		// TODO Auto-generated method stub
		return null;
	}

}
