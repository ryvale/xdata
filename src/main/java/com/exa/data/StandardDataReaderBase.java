package com.exa.data;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class StandardDataReaderBase<_FIELD extends Field> implements DataReader<_FIELD> {
	protected Map<String, _FIELD> fields = new LinkedHashMap<>();

	@Override
	public boolean execute() throws DataException {
		return next();
	}

	@Override
	public void executeFieldsAction(FieldAction<_FIELD> fa) throws DataException {
		
		for(_FIELD field : fields.values()) {
			fa.execute(field);
		}
	}

	@Override
	public _FIELD getField(String name) {
		return fields.get(name);
	}

	@Override
	public boolean containsField(String fieldName) {
		return fields.containsKey(fieldName);
	}
	
	@Override
	public abstract StandardDataReaderBase<_FIELD> cloneDR() throws DataException;
	
	@Override
	public DataReader<?> getSubDataReader(String name) {
		return null;
	}

	@Override
	public Object getObject(String fieldName) throws DataException {
		_FIELD field = fields.get(fieldName);
		if(field == null) throw new DataException(String.format("Unknown field name %s", fieldName));
		
		if("date".equals(field.getType()) || "datetime".equals(field.getType())) return getDate(fieldName);
		
		if("double".equals(field.getType()) || "float".equals(field.getType()) || "decimal".equals(field.getType())) return getDouble(fieldName);
		
		return getString(fieldName);
	}
	
	
	
}
