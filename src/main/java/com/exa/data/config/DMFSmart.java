package com.exa.data.config;

import java.util.Map;

import javax.sql.DataSource;

import com.exa.data.DataReader;
import com.exa.data.SmartDataReader;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public class DMFSmart extends DataManFactory {
	
	private Map<String, DataSource> dataSources;
	private String defaultDataSource;

	public DMFSmart(FilesRepositories filesRepos, Map<String, DataSource> dataSources, String defaultDataSource) {
		
		super(filesRepos, (id, context) -> {
			if("rootDr".equals(id)) return "DataReader";
			if("rootOv".equals(id)) return "ObjectValue";
			String p[] = context.split("[.]");
			if(p.length>=3 && ("this".equals(id) || getDRVariableName(id).equals(p[2]))) return "DataReader";
			if(p.length>=3 && "sourceDr".equals(id)) return "DataReader";
			return null;
		});
		
		this.dataSources = dataSources;
		this.defaultDataSource = defaultDataSource;
	}

	@Override
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> config, XPEvaluator eval, VariableContext variableContext) throws ManagedException {
		return new SmartDataReader(name, config, eval, variableContext, filesRepos, dataSources, defaultDataSource);
	}

}
