package com.exa.data;

import java.util.Date;
import java.util.Map;

import com.exa.data.config.utils.DMutils;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;

public class MapReader extends StandardDataReaderBase<Field> {
	//protected Map<String, Field> fields = new LinkedHashMap<>();

	public static interface MapGetter {
		Map<String, ?> get();
	}
	
	private MapGetter mapGetter;
	
	private Map<String, ?> data;
	
	protected int _lineVisited = 0;
	
	private ObjectValue<XPOperand<?>> config;

	public MapReader(String name, XPEvaluator evaluator, VariableContext variableContext, ObjectValue<XPOperand<?>> config, DMutils dmu, MapGetter mapGetter) {
		super(name, evaluator, variableContext, dmu);
		this.config = config;
		
		this.mapGetter = mapGetter;
	}
	/*public MapReader(Map<String, Field> fields, MapGetter mapGetter) {
		super();
		this.fields = fields;
		this.mapGetter = mapGetter;
	}*/

	@Override
	public boolean next() throws DataException {
		if(_lineVisited > 0) return false;
		++_lineVisited;

		return true;
	}

	@Override
	public String getString(String fieldName) throws DataException {
		Field field = fields.get(fieldName);
		if(field == null) return null;
		
		if(!"string".equals(field.getType())) throw new DataException(String.format("the field %s is not a string in the data reader %s", fieldName, name));
		
		Object v = data.get(fieldName);
		if(v == null) return null;
		
		return v.toString();
	}

	@Override
	public Integer getInteger(String fieldName) throws DataException {
		Field field = fields.get(fieldName);
		if(field == null) return null;
		
		if(!"int".equals(field.getType()) && !"integer".equals(field.getType())) throw new DataException(String.format("the field %s is not an integer in data reader %", fieldName, name));
		
		Object v = data.get(fieldName);
		if(v == null) return null;
		
		return (Integer)v;
	}

	@Override
	public Date getDate(String fieldName) throws DataException {
		Field field = fields.get(fieldName);
		if(field == null) return null;
		
		if(!"date".equals(field.getType()) && !"datetime".equals(field.getType())) throw new DataException(String.format("the field %s is not a date in a data reader %s", fieldName, name));

		Object v = data.get(fieldName);
		if(v == null) return null;
		
		return (Date)v;
	}

	@Override
	public Double getDouble(String fieldName) throws DataException {
		Field field = fields.get(fieldName);
		if(field == null) return null;
		
		if(!"float".equals(field.getType()) && !"decimal".equals(field.getType()) && !"double".equals(field.getType())) throw new DataException(String.format("the field %s is not a float in data reader %s", fieldName, name));
		
		Object v = data.get(fieldName);
		if(v == null) return null;
		
		Number n = (Number)v;
		
		return n.doubleValue();
	}

	@Override
	public boolean open() throws DataException {
		try {
			ObjectValue<XPOperand<?>> fm = config.getRequiredAttributAsObjectValue("fields");
			Value<?, XPOperand<?>> vlFields = fm.getRequiredAttribut("items");
			ArrayValue<XPOperand<?>> avFields = vlFields.asArrayValue();
		 	if(avFields == null) {
		 		ObjectValue<XPOperand<?>> ovFields = fm.getRequiredAttributAsObjectValue("items");
				Map<String, Value<?,XPOperand<?>>> mpFields = ovFields.getValue();
				
				for(String fname : mpFields.keySet()) {
					Value<?, XPOperand<?>> vlField = mpFields.get(fname);
					
					String type;
					BooleanValue<?> blField = vlField.asBooleanValue();
					if(blField == null) {
						StringValue<XPOperand<?>> sv = vlField.asStringValue();
						if(sv == null) {
							ObjectValue<XPOperand<?>> ov = vlField.asRequiredObjectValue();

							type = ov.getAttributAsString("type", "string");
						}
						else {
							type = "string";
						}
					}
					else {
						type = "string";
					}
					
					Field field = new Field(fname, type);
					
					fields.put(fname, field);
				}
		 	}
		 	else {
		 		
		 	}
		 	
		 	for(DataReader<?> dr : dmu.getReaders().values()) {
				dr.open();
			}
		} catch (ManagedException e) {
			throw new DataException(e);
		}
		data = mapGetter.get();
		return true;
	}

	@Override
	public void close() throws DataException {
		data = null;
		_lineVisited = 0;
		for(DataReader<?> dr : dmu.getReaders().values()) {
			try { dr.close(); } catch(DataException e) {}
		}
		
	}

	@Override
	public int lineVisited() {
		return _lineVisited;
	}

	@Override
	public boolean isOpen() {
		return _lineVisited  > 0;
	}

	@Override
	public MapReader cloneDM() throws DataException {
		return new MapReader(name, evaluator, variableContext, config, dmu, mapGetter);
	}

	
	
	
}
