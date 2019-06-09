package com.exa.data.config;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.XLiteralDataReader;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.expression.parsing.Parser.UnknownIdentifierValidation;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public class DMFXLiteral extends DataManFactory {

	public DMFXLiteral(FilesRepositories filesRepos, UnknownIdentifierValidation uiv) {
		super(filesRepos, uiv);
	}
	
	public DMFXLiteral(FilesRepositories filesRepos) {
		super(filesRepos);
	}

	@Override
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext vc) throws ManagedException {
		return new XLiteralDataReader(name, eval, vc, ovEntity);
	}

	@Override
	public DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext vc, DataReader<?> drSource, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		// TODO Auto-generated method stub
		return null;
	}

}
