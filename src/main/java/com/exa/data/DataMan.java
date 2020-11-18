package com.exa.data;

import com.exa.expression.eval.XPEvaluator;

public interface DataMan {
	
	boolean containsField(String fieldName);
	
	boolean execute() throws DataException;
	
	boolean open() throws DataException;
	
	void close() throws DataException;

	boolean isOpen();
	
	DataMan cloneDM() throws DataException;
	
	XPEvaluator getEvaluator();
	
	DataReader<?> asDataReader();
	
	DataWriter<?> asDataWriter();
}
