package com.exa.data.config;


import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.LibreDataReader;
import com.exa.data.XADataSource;
import com.exa.data.config.utils.DMutils;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;


public class DMFLibre extends DataManFactory {
	
	public DMFLibre(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource) {
		super(filesRepos, dataSources, defaultDataSource);
	}

	@Override
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext variableContext, DMutils dmu) throws ManagedException {
		return new LibreDataReader(name, ovEntity, eval, variableContext, dmu);
	}

	@Override
	public DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval,
			VariableContext vc, DataReader<?> drSource, DMutils dmu, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		// TODO Auto-generated method stub
		return null;
	}

	

}
