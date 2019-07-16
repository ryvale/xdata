package com.exa.data.config;

import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.RowToFieldDataReader;
import com.exa.data.XADataSource;
import com.exa.data.config.utils.DMUtils;

import com.exa.expression.XPOperand;

import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public class DMFRowToField extends DataManFactory {

	public DMFRowToField(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMUSetup dmuSetup) {
		super(filesRepos, dataSources, defaultDataSource, dmuSetup, (id, context) -> {
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
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, DMUtils dmu) throws ManagedException {
		return new RowToFieldDataReader(name, ovEntity, filesRepos, dataSources, defaultDataSource, dmu, dmuSetup);
	}

	@Override
	public DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity/*, XPEvaluator eval, VariableContext vc*/, DataReader<?> drSource, DMUtils dmu, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		// TODO Auto-generated method stub
		return null;
	}

}
