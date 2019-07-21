package com.exa.data;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.exa.data.config.utils.DMUtils;
import com.exa.data.config.utils.DataUserException;
import com.exa.expression.XPOperand;

import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;

public class XLiteralDataReader extends StandardDataReaderBase<Field> {
	
	private final static DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	static final Set<String> allowedTypes = new HashSet<>();
	
	static {
		allowedTypes.add("string");
		allowedTypes.add("date");
		allowedTypes.add("datetime");
		allowedTypes.add("int");
		allowedTypes.add("integer");
		allowedTypes.add("float");
		allowedTypes.add("double");
		allowedTypes.add("decimal");
		allowedTypes.add("boolean");
	}
	
	private ObjectValue<XPOperand<?>> config;
	
	private List<Map<String, Value<?, XPOperand<?>>>> rows = new ArrayList<>();
	private int rowIndex = -1;
	
	private Value<?, XPOperand<?>> vlBreak;
	private Value<?, XPOperand<?>> vlBreakThrowError;
	private Value<?, XPOperand<?>> vlBreakUserMessage;
	
	public XLiteralDataReader(String name, ObjectValue<XPOperand<?>> config, DMUtils dmu) {
		super(name, dmu);
		this.config = config;
	}

	@Override
	public boolean next() throws DataException {
		try {
			if(vlBreak.asBoolean()) {
				if(vlBreakThrowError == null) return false;
				String errMess = vlBreakThrowError.asString();
				String userMessage = vlBreakUserMessage == null ? null : vlBreakUserMessage.asString();
				
				if(errMess == null) return false;
				
				throw new  DataUserException(errMess, userMessage);
			}
		} catch (ManagedException e) {
			if(e instanceof DataException) throw (DataException)e;
			throw new DataException(e);
		}
		if(rowIndex + 1 >= rows.size()) return false;
		++rowIndex;
		return true;
	}

	@Override
	public String getString(String fieldName) throws DataException {
		Field field = fields.get(fieldName);
		if(field == null) return null;
		
		if(!"string".equals(field.getType())) throw new DataException(String.format("the field %s is not a string in the data reader %s", fieldName, name));
		
		try {
			return rows.get(rowIndex).get(fieldName).asString();
		} catch (ManagedException e) {
			throw new DataException(e);
		}
	}

	@Override
	public Integer getInteger(String fieldName) throws DataException {
		Field field = fields.get(fieldName);
		if(field == null) return null;
		
		if(!"int".equals(field.getType()) && !"integer".equals(field.getType())) throw new DataException(String.format("the field %s is not an integer in data reader %", fieldName, name));
		
		try {
			return rows.get(rowIndex).get(fieldName).asInteger();
		} catch (ManagedException e) {
			throw new DataException(e);
		}
	}

	@Override
	public Date getDate(String fieldName) throws DataException {
		Field field = fields.get(fieldName);
		if(field == null) return null;
		
		if(!"date".equals(field.getType()) && !"datetime".equals(field.getType())) throw new DataException(String.format("the field %s is not a date in a data reader %s", fieldName, name));
		
		
		try {
			String v = rows.get(rowIndex).get(fieldName).asString();
			if(v == null) return null;
			
			return DF.parse(v);
		} catch (ManagedException|ParseException e) {
			throw new DataException(e);
		}
	}

	@Override
	public Double getDouble(String fieldName) throws DataException {
		Field field = fields.get(fieldName);
		if(field == null) return null;
		
		if(!"float".equals(field.getType()) && !"decimal".equals(field.getType()) && !"double".equals(field.getType())) throw new DataException(String.format("the field %s is not a float in data reader %s", fieldName, name));
		
		return rows.get(rowIndex).get(fieldName).asDecimalValue().getValue();
	}

	@Override
	public boolean open() throws DataException {
		try {
			vlBreak = config.getAttribut("break");
			if(vlBreak == null) vlBreak = new BooleanValue<>(Boolean.FALSE);
			else {
				ObjectValue<XPOperand<?>> ovBreak = vlBreak.asObjectValue();
				if(ovBreak == null) {
					if(!"boolean".equals(vlBreak.typeName())) throw new DataException(String.format("The property 'break' should be a boolean or an object in data reader named '%'", name));
				} else {
					vlBreak = ovBreak.getRequiredAttribut("condition");
					if(!"boolean".equals(vlBreak.typeName())) throw new DataException(String.format("The property 'break.condtion' should be a boolean in data reader named '%'", name));
					
					vlBreakUserMessage = ovBreak.getAttribut("userMessage");
					if(vlBreakUserMessage != null)
						if(!"string".equals(vlBreakUserMessage.typeName())) throw new DataException(String.format("The property 'break.userMessage' should be a string in data reader named '%'", name));
					
					vlBreakThrowError = ovBreak.getAttribut("throwError");
					if(vlBreakThrowError != null) {
						if(!"string".equals(vlBreakThrowError.typeName())) throw new DataException(String.format("The property 'break.throwError' should be a string in data reader named '%'", name));
					}
					else vlBreakThrowError = vlBreakUserMessage;
					
				}
			}
			
			ObjectValue<XPOperand<?>> fm = config.getAttributAsObjectValue("fields");
			if(fm != null) {
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
			}
		 	
		 	ArrayValue<XPOperand<?>> avRows = config.getAttributAsArrayValue("rows");
		 	if(avRows == null) return true;
		 	
		 	dmu.executeBeforeConnectionActions();
		 	
		 	List<Value<?, XPOperand<?>>> lstRows = avRows.getValue();
		 	int nb = lstRows.size();
		 	
		 	for(int i=0; i<nb; i++) {
		 		Value<?, XPOperand<?>> vlRow = lstRows.get(i);
		 		ObjectValue<XPOperand<?>> ovRow = vlRow.asObjectValue();
		 		if(ovRow == null) throw new DataException(String.format("Object value expected in Data reader %s rows at line %d", name, i));
		 		
		 		Map<String, Value<?, XPOperand<?>>> mpRow = ovRow.getValue();
		 		for(String fname : mpRow.keySet()) {
		 			Field field = fields.get(fname);
		 			
		 			String vType = mpRow.get(fname).typeName();

		 			if(field == null) {
		 				field = new Field(fname, vType);
		 				fields.put(fname, field);
		 			}
		 			
		 			if(field.getType().equals(vType)) continue;
		 			
		 			if(allowedTypes.contains(field.getType()) && allowedTypes.contains(vType)) continue;
		 			
		 			throw new DataException(String.format("Type mismatch for field %s of data reader %s. (not %s <=> %s)", fname, name, field.getType(), vType));
		 		}
		 		
		 		rows.add(mpRow);
		 		
		 	}

		 	
		} catch (ManagedException e) {
			throw new DataException(e);
		}
		return true;
	}

	@Override
	public void close() throws DataException {
		dmu.clean();
		rowIndex = -1;
		rows.clear();
	}

	@Override
	public boolean isOpen() {
		return rowIndex != -1;
	}

	@Override
	public int lineVisited() {
		return rowIndex + 1;
	}

	@Override
	public XLiteralDataReader cloneDM() throws DataException {
		return new XLiteralDataReader(name/*, evaluator, variableContext*/, config, dmu);
	}

}
