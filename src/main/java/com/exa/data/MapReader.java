package com.exa.data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.exa.data.config.utils.DMUtils;

import com.exa.expression.XPOperand;
import com.exa.lang.parsing.Computing;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;

public class MapReader extends StandardDataReaderBase<DynamicField> {
	public static final String DFSTR_STD = "yyyy-MM-dd HH:mm:ss";
	
	protected final static Set<String> expTypes = new HashSet<>();
	
	private boolean dataInBuffer = false;

	public static interface MapGetter {
		Map<String, ?> get();
	}
	
	static {
		expTypes.add("default");expTypes.add("map");expTypes.add("value");expTypes.add("from-string");
	}
	
	private MapGetter mapGetter;
	
	private Map<String, ?> data;
	
	protected int _lineVisited = 0;
	
	private ObjectValue<XPOperand<?>> config;

	public MapReader(String name, ObjectValue<XPOperand<?>> config, DMUtils dmu, MapGetter mapGetter) {
		super(name, dmu);
		this.config = config;
		
		this.mapGetter = mapGetter;
	}

	@Override
	public boolean next() throws DataException {
		if(_lineVisited > 0) return dataInBuffer = false;
		++_lineVisited;

		return dataInBuffer = true;
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
		DynamicField field = fields.get(fieldName);
		if(field == null) return null;
		
		if(!"date".equals(field.getType()) && !"datetime".equals(field.getType())) throw new DataException(String.format("the field %s is not a date in a data reader %s", fieldName, name));

		Object v;
		String expType = field.getExpType();
		
		try {
			if("map".equals(expType)) {
				v = data.get(field.getVlExp().asString());
			}
			else if("value".equals(expType)) {
				v = field.getVlExp().asString();
			}
			else {
				v = data.get(fieldName);
			}
			if(v == null) return null;
			
			if("from-string".equals(field.getExpType()) || field.getFrom() != null /*&& ("string".equals(field.getFrom().asString()))*/) {
				String from = field.getFrom().asRequiredString();
				
				if("adjusted-string".equals(from) || "string".equals(from)) {
					
					if("adjusted-string".equals(from)) 
						v=v.toString().substring(0, 19).replace('T', ' ');
					
				
					String format = field.getFromFormat() == null ? DFSTR_STD : field.getFromFormat().asString();
					return new SimpleDateFormat(format).parse((String)v);
				}
				else {
					String format = field.getFromFormat() == null ? DFSTR_STD : field.getFromFormat().asString();
					return new SimpleDateFormat(format).parse((String)v);
				}
			}
			
		} catch (ParseException|ManagedException e) {
			throw new DataException(e);
		}
		
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
					if(Computing.PRTY_ENTITY.equals(fname)) continue;
					
					Value<?, XPOperand<?>> vlField = mpFields.get(fname);
					
					Value<?, XPOperand<?>> vlExp, vlCondition, vlFrom = null, vlFromFormat = null;
					String type, expType;
					BooleanValue<?> blField = vlField.asBooleanValue();
					if(blField == null) {
						StringValue<XPOperand<?>> sv = vlField.asStringValue();
						if(sv == null) {
							ObjectValue<XPOperand<?>> ov = vlField.asRequiredObjectValue();
							
							vlExp = ov.getAttribut("exp");
							type = ov.getAttributAsString("type", "string");
							expType = ov.getAttributAsString("expType", "map");
							vlCondition = ov.getAttribut("condition");
							vlFrom = ov.getAttribut("from");
							vlFromFormat = ov.getAttribut("fromFromat");
							
							if(vlExp == null) vlExp = new StringValue<>(fname);
							
							if(vlFrom != null) {
								if(!"string".equals(vlFrom.typeName()))
									throw new ManagedException(String.format("String expression expected as value of 'from' propertu for the entity %s", name));
							}
							
							if(vlFromFormat != null) {
								if(!"string".equals(vlFrom.typeName()))
									throw new ManagedException(String.format("String expression expected as value of 'fromFormat' propertu for the entity %s", name));
							}
							
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
					
					if("default".equals(expType)) expType = "map";
					
					DynamicField field = new DynamicField(fname, type, expType);
					field.setVlExp(vlExp);
					field.setVlCondition(vlCondition);
					
					field.setFrom(vlFrom);
					
					field.setFromFormat(vlFromFormat);
					
					fields.put(fname, field);				
				}
		 	}
		 	else {
		 		
		 	}
		 	dmu.executeBeforeConnectionActions();
		 	
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
		
		dmu.clean();
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
		return new MapReader(name, config, dmu, mapGetter);
	}

	@Override
	public boolean dataInBuffer() {
		return dataInBuffer;
	}

	
	
	
}
