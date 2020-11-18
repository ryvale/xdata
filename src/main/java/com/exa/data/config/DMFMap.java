package com.exa.data.config;

import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.MapDataSource;
import com.exa.data.MapReader;
import com.exa.data.XADataSource;
import com.exa.data.config.utils.DMUtils;

import com.exa.expression.XPOperand;

import com.exa.expression.parsing.Parser.UnknownIdentifierValidation;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public class DMFMap  extends DataManFactory {
	
	public DMFMap(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMUSetup dmuSetup) {
		super(filesRepos, dataSources, defaultDataSource, dmuSetup);
	}
	
	public DMFMap(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMUSetup dmuSetup, UnknownIdentifierValidation uiv) {
		super(filesRepos, dataSources, defaultDataSource, dmuSetup, uiv);
	}
	
	public DMFMap(DMFGeneral dmuDmf) {
		super(dmuDmf);
	}

	@Override
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, DMUtils dmu) throws ManagedException {
		String dsName = ovEntity.getAttributAsString("dataSource");
		
		if(dsName == null) dsName = defaultDataSource;
		
		if(dsName == null) throw new ManagedException(String.format("No data source provided."));
		
		XADataSource xaDS = dataSources.get(dsName);
		if(xaDS == null) throw new ManagedException(String.format("The data source '%s' specified is not present.", dsName));
		
		MapDataSource ds = xaDS.asMapDataSource();
		
		return new MapReader(name, ovEntity, dmu, ds.getMapGetter());
	}

	@Override
	public DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity/*, XPEvaluator eval, VariableContext vc*/, DataReader<?> drSource, DMUtils dmu, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		// TODO Auto-generated method stub
		return null;
	}

}
