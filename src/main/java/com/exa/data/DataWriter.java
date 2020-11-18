package com.exa.data;

public interface DataWriter<_FIELD extends Field> extends DataMan  {
	
	int update(DataReader<?> dr) throws DataException;
	
	boolean open() throws DataException;
	
	void close() throws DataException;
	
	@Override
	DataWriter<_FIELD> cloneDM() throws DataException;
}
