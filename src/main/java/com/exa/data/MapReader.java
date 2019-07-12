package com.exa.data;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.exa.data.config.utils.DMutils;

import com.exa.expression.XPOperand;

import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;

public class MapReader extends StandardDataReaderBase<DynamicField> {
	
	protected final static Set<String> expTypes = new HashSet<>();

	public static interface MapGetter {
		Map<String, ?> get();
	}
	
	static {
		expTypes.add("default");expTypes.add("map");expTypes.add("value");
	}
	
	private MapGetter mapGetter;
	
	private Map<String, ?> data;
	
	protected int _lineVisited = 0;
	
	private ObjectValue<XPOperand<?>> config;

	public MapReader(String name/*, XPEvaluator evaluator, VariableContext variableContext*/, ObjectValue<XPOperand<?>> config, DMutils dmu, MapGetter mapGetter) {
		super(name/*, evaluator, variableContext*/, dmu);
		this.config = config;
		
		this.mapGetter = mapGetter;
	}

	@Override
	public boolean next() throws DataException {
		if(_lineVisited > 0) return false;
		++_lineVisited;

		return true;
	}

	@Override
	public String getString(String fieldName) throws DataException {
		DynamicField field = fields.get(fieldName);
		if(field == null) return null;
		
		if(!"string".equals(field.getType())) throw new DataException(String.format("the field %s is not a string in the data reader %s", fieldName, name));
		
		String expType = field.getExpType();
		
		try {
			if("map".equals(expType)) {
				Object v = data.get(field.getVlExp().asString());
				if(v == null) return null;
				
				return v.toString();
			}
			
			if("value".equals(expType)) {
				if(!"string".equals(field.getVlExp().typeName())) throw new DataException(String.format("The field %s is not a string in reader %s", fieldName, name));
				return field.getVlExp().asString();
			}
		}
		catch (ManagedException e) {
			throw new DataException(e);
		}
		
		throw new DataException(String.format("The field expression type %s for field %s is not yet managed in reader %s", field.getExpType(), fieldName, name));
	}

	@Override
	public Integer getInteger(String fieldName) throws DataException {
		DynamicField field = fields.get(fieldName);
		
		if(field == null) return null;
		
		if(!"int".equals(field.getType()) && !"integer".equals(field.getType())) throw new DataException(String.format("the field %s is not an integer in data reader %", fieldName, name));
		String expType = field.getExpType();
		
		try {
			if("map".equals(expType)) {
				Object v = data.get(field.getVlExp().asString());
				if(v == null) return null;
				
				return (Integer)v;
			}
			
			if("value".equals(expType)) {
				if(!("int".equals(field.getVlExp().typeName()) || "integer".equals(field.getVlExp().typeName()))) throw new DataException(String.format("The field %s is not an integer in reader %s", fieldName, name));
				return field.getVlExp().asInteger();
			}
		}
		catch (ManagedException e) {
			throw new DataException(e);
		}
		
		throw new DataException(String.format("The field expression type %s for field %s is not yet managed in reader %s", field.getExpType(), fieldName, name));
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
		DynamicField field = fields.get(fieldName);
		if(field == null) return null;
		
		if(!"float".equals(field.getType()) && !"decimal".equals(field.getType()) && !"double".equals(field.getType())) throw new DataException(String.format("the field %s is not a float in data reader %s", fieldName, name));
		
		String expType = field.getExpType();
		
		try {
			if("map".equals(expType)) {
				Object v = data.get(field.getVlExp().asString());
				if(v == null) return null;
				
				Number n = (Number)v;
				
				return n.doubleValue();
			}
		} catch (ManagedException e) {
			throw new DataException(e);
		}
		
		throw new DataException(String.format("The field expression type %s for field %s is not yet managed in reader %s", field.getExpType(), fieldName, name));
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
					
					Value<?, XPOperand<?>> vlExp, vlCondition;
					String type, expType;
					BooleanValue<?> blField = vlField.asBooleanValue();
					if(blField == null) {
						StringValue<XPOperand<?>> sv = vlField.asStringValue();
						if(sv == null) {
							ObjectValue<XPOperand<?>> ov = vlField.asRequiredObjectValue();
							
							vlExp = ov.getRequiredAttribut("exp");
							type = ov.getAttributAsString("type", "string");
							expType = ov.getAttributAsString("expType", "map");
							vlCondition = ov.getAttribut("condition");
							if(vlCondition == null) vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
							else {
								CalculableValue<?, XPOperand<?>> clCondition = vlCondition.asCalculableValue();
								if(clCondition == null) {
									if(vlCondition.asBooleanValue() == null) throw new ManagedException(String.format("Boolean expression expected as value of 'condition' propertu for the entity %s", name));
								}
								else {
									if(!"boolean".equals(clCondition.typeName())) throw new ManagedException(String.format("Boolean expression expected as value of 'condition' propertu for the entity %s", name));
								}
								
							}
						}
						else {
							vlExp = sv;
							expType = "map";
							type = "string";
							vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
						}
					}
					else {
						vlExp = new StringValue<>(fname);
						expType = "map";
						type = "string";
						vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
					}
					
					if(!expTypes.contains(expType)) throw new DataException(String.format("Invalid expresssion type '%' for field '%s'", expType, fname));
					
					//if("value".equals(expType) && !"string".equals(type)) throw new DataException(String.format("For the expression type 'value' the field type should be instead of %s for field %s", type, fname));
					
					if("default".equals(expType)) expType = "map";
					
					DynamicField field = new DynamicField(fname, type, expType);
					field.setVlExp(vlExp);
					field.setVlCondition(vlCondition);
					
					fields.put(fname, field);				}
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
		return new MapReader(name/*, evaluator, variableContext*/, config, dmu, mapGetter);
	}

	
	
	
}
