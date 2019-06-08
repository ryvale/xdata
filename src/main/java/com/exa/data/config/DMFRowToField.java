package com.exa.data.config;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.RowToFieldDataReader;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public class DMFRowToField extends DataManFactory {
	private Map<String, DataSource> dataSources = new HashMap<>();
	
	private String defaultDataSource;

	public DMFRowToField(FilesRepositories filesRepos, Map<String, DataSource> dataSources, String defaultDataSource) {
		super(filesRepos, (id, context) -> {
			if(!"sourceDr".equals(id)) {
				String p[] = context.split("[.]");
				if(p.length<3 || !getDRVariableName(p[2]).equals(id)) return null;
			}
			return "DataReader";
		});

		this.dataSources = dataSources;
		this.defaultDataSource = defaultDataSource;
	}

	@Override
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator evaluator, VariableContext variableContext) throws ManagedException {
	
		return new RowToFieldDataReader(name, ovEntity, evaluator, variableContext, filesRepos, dataSources, defaultDataSource);
	}

	@Override
	public DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval,
			VariableContext vc, DataReader<?> drSource) throws ManagedException {
		// TODO Auto-generated method stub
		return null;
	}

}
