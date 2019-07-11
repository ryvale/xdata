package com.exa.data.config;

import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.XADataSource;
import com.exa.data.config.utils.DMutils;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.expression.parsing.Parser.UnknownIdentifierValidation;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public class DMFGeneral extends DataManFactory {
	
	protected String defaultType;

	public DMFGeneral(String defaultType, FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource) {
		super(filesRepos, dataSources, defaultDataSource);
		this.defaultType = defaultType;
		
	}
	public DMFGeneral(String defaultType, FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, UnknownIdentifierValidation uiv) {
		super(filesRepos, dataSources, defaultDataSource, uiv);
		
		this.defaultType = defaultType;
	}


	@Override
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext vc, DMutils dmu) throws ManagedException {
		String type = ovEntity.getAttributAsString("type", defaultType);
		
		DataManFactory dmf = dmFactories.get(type);
		
		if(dmf == null) throw new ManagedException(String.format("Invalid type '%s'", type));
		
		return dmf.getDataReader(name, ovEntity, eval, vc, dmu);
	}

	@Override
	public DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext vc, DataReader<?> drSource, DMutils dmu, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		String type = ovEntity.getAttributAsString("type", defaultType);
		
		DataManFactory dmf = dmFactories.get(type);
		
		if(dmf == null) throw new ManagedException(String.format("Invalid type '%s'", type));
		
		return dmf.getDataWriter(name, ovEntity, eval, vc, drSource, dmu, preventInsertion, preventUpdate);
	}

}
