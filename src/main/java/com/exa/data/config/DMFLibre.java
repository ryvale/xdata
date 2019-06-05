package com.exa.data.config;


import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.LibreDataReader;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;


public class DMFLibre extends DataManFactory {
	
	public DMFLibre(FilesRepositories filesRepos) {
		super(filesRepos);
	}

	@Override
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext variableContext) throws ManagedException {
		return new LibreDataReader(name, ovEntity, eval, variableContext);
	}

	@Override
	public DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval,
			VariableContext vc) throws ManagedException {
		// TODO Auto-generated method stub
		return null;
	}

	

}
