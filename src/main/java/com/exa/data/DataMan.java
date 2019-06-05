package com.exa.data;

import com.exa.expression.eval.XPEvaluator;

public interface DataMan {
	boolean containsField(String fieldName);
	
	boolean execute() throws DataException;
	
	DataMan cloneDM() throws DataException;
	
	void setEvaluator(XPEvaluator evaluator);
	
	XPEvaluator getEvaluator();
	
	DataReader<?> asDataReader();
	
	DataWriter<?> asDataWriter();
}
