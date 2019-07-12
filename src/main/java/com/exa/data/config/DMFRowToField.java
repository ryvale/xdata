package com.exa.data.config;

import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.RowToFieldDataReader;
import com.exa.data.XADataSource;
import com.exa.data.config.utils.DMutils;

import com.exa.expression.XPOperand;

import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public class DMFRowToField extends DataManFactory {

	public DMFRowToField(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource) {
		super(filesRepos, dataSources, defaultDataSource, (id, context) -> {
			if(!"sourceDr".equals(id)) {
				String p[] = context.split("[.]");
				if(p.length<3 || !getDRVariableName(p[2]).equals(id)) return null;
			}
			return "DataReader";
		});
	}
	
	public DMFRowToField(DMFGeneral dmuDmf) {
		super(dmuDmf);
	}

	@Override
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity/*, XPEvaluator evaluator, VariableContext variableContext*/, DMutils dmu) throws ManagedException {
		return new RowToFieldDataReader(name, ovEntity/*, evaluator, variableContext*/, filesRepos, dataSources, defaultDataSource, dmu);
	}

	@Override
	public DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity/*, XPEvaluator eval, VariableContext vc*/, DataReader<?> drSource, DMutils dmu, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		// TODO Auto-generated method stub
		return null;
	}

}
