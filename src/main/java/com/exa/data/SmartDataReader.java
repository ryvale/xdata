package com.exa.data;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class SmartDataReader extends StandardDataReaderBase<Field> {
	protected Map<String, DataReader<?>> mainReaders = new LinkedHashMap<>();
	protected Map<String, DataMan> afterMainActions = new LinkedHashMap<>();
	protected Map<String, DataMan> oneTimeActions = new LinkedHashMap<>();
	protected Map<String, DataMan> alwaysActions = new LinkedHashMap<>();
	protected Map<String, DataMan>  afterMainOneTimeActions = new LinkedHashMap<>();
	
	protected Map<String, SmartDataReader> subReaders = new LinkedHashMap<>();
	
	protected DataReader<?> currentMainReader = null;
	protected boolean dataRead = false;

	
	private Iterator<DataReader<?>> drIndex = null; 
	
	
	public void addMainDataReader(String name, DataReader<?> dataReader) throws DataException {
		mainReaders.put(name, dataReader);
		
		dataReader.executeFieldsAction(field -> {
			String fieldName = field.getName();
			if(fields.containsKey(fieldName)) return;
			
			fields.put(fieldName, new Field(fieldName, field.getType()));
		});
	}

	@Override
	public boolean next() throws DataException {
		
		while(!currentMainReader.next()) {
			if(drIndex.hasNext()) {
				currentMainReader.close();
				currentMainReader = drIndex.next();
				currentMainReader.open();
			}
			else currentMainReader.close();
			return dataRead = false;
		}
		
		dataRead = true;
		
		for(DataMan dm : afterMainActions.values()) dm.execute();
		
		return true;
	}

	@Override
	public Field getField(String name) {
		return fields.get(name);
	}

	@Override
	public String getString(String fieldName) throws DataException {
		if(currentMainReader.containsField(fieldName)) return currentMainReader.getString(fieldName);
		return null;
	}

	@Override
	public boolean open() throws DataException {
		drIndex = mainReaders.values().iterator();
		
		if(!drIndex.hasNext()) return dataRead = false;
		currentMainReader.open();
		currentMainReader = drIndex.next();
		
		return dataRead = true;
		
	}

	@Override
	public void close() throws DataException {

	}

	@Override
	public boolean isOpen() {
		return drIndex != null;
	}

	@Override
	public Date getDate(String fieldName) throws DataException {
		if(currentMainReader.containsField(fieldName)) return currentMainReader.getDate(fieldName);
		return null;
	}

	@Override
	public Double getDouble(String fieldName) throws DataException {
		if(currentMainReader.containsField(fieldName)) return currentMainReader.getDouble(fieldName);
		return null;
	}

	@Override
	public SmartDataReader cloneDR() {
		
		return null;
	}

	@Override
	public Double getObject(String fieldName) throws DataException {
		// TODO Auto-generated method stub
		return null;
	}
	 

}
