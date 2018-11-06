package com.exa.data;

import com.exa.expression.eval.XPEvaluator;

public interface DataMan {
	boolean execute() throws DataException;
	
	DataMan cloneDR() throws DataException;
	
	void setEvaluator(XPEvaluator evaluator);
	
	XPEvaluator getEvaluator();
	
	DataReader<?> asDataReader();
}
