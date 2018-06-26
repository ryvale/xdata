package com.exa.data;

import java.util.Date;

public interface DataReader<_FIELD extends Field> extends DataMan {
	_FIELD getField(String name);
	boolean containsField(String fieldName);
	
	void executeFieldsAction(FieldAction<_FIELD> fa) throws DataException;
	boolean next() throws DataException;
	
	String getString(String fieldName) throws DataException;
	
	Date getDate(String fieldName) throws DataException;
	
	Double getDouble(String fieldName) throws DataException;
	
	Object getObject(String fieldName) throws DataException;
	
	boolean open() throws DataException;
	
	void close() throws DataException;
	
	boolean isOpen();
	
	@Override
	DataReader<_FIELD> cloneDR() throws DataException;
	
	DataReader<?> getSubDataReader(String name);

}
