package com.exa.data;

import java.util.Date;

public interface DataReader<_FIELD extends Field> extends DataMan {
	public static boolean debugOn = false;
	
	_FIELD getField(String name);
	
	void executeFieldsAction(FieldAction<_FIELD> fa) throws DataException;
	boolean next() throws DataException;
	
	boolean dataInBuffer();
	
	String getString(String fieldName) throws DataException;
	
	Integer getInteger(String fieldName) throws DataException;
	
	Date getDate(String fieldName) throws DataException;
	
	Double getDouble(String fieldName) throws DataException;
	
	Object getObject(String fieldName) throws DataException;
	
	@Override
	DataReader<_FIELD> cloneDM() throws DataException;
	
	DataMan getSubDataMan(String name);
	
	int lineVisited();
	
	
	DataReader<?> getParent();
	
	void setParent(DataReader<?> parent);
	

}
