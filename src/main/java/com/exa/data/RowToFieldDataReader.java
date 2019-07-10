package com.exa.data;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import com.exa.data.config.DataManFactory;
import com.exa.data.config.utils.DMutils;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.DecimalValue;
import com.exa.utils.values.IntegerValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;


public class RowToFieldDataReader extends StandardDRWithDSBase<RowToFieldDataReader.Field> {
	public static class Field extends com.exa.data.Field {
		protected Value<?, XPOperand<?>> valueExp;
		
		protected Value<?, XPOperand<?>> ifExp;
		
		protected Value<?, XPOperand<?>> defaultValueExp;

		public Field(String name, String type, Value<?, XPOperand<?>> ifExp, Value<?, XPOperand<?>> valueExp, Value<?, XPOperand<?>> defaultValueExp) {
			super(name, type);
			
			this.ifExp = ifExp;
			this.valueExp = valueExp;
			this.defaultValueExp = defaultValueExp;
		}
	}
	
	protected DataReader<?> drSource = null;
	
	protected Value<?, XPOperand<?>> valueExp;
	
	protected Value<?, XPOperand<?>> defaultIfExp;
	
	protected Value<?, XPOperand<?>> typeExp;
	
	protected Value<?, XPOperand<?>> defaultValueExp;
	
	protected Map<String, Object> values = new LinkedHashMap<>();
	
	public RowToFieldDataReader(String name, ObjectValue<XPOperand<?>> config, XPEvaluator evaluator, VariableContext variableContext, FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMutils dmu) {
		super(name, config, evaluator, variableContext, filesRepos, dataSources, defaultDataSource, dmu);
	}

	@Override
	public boolean next() throws DataException {
		
		while(drSource.next()) {
			for(Field field : fields.values()) {
				try {
					
					if(field.ifExp.asBoolean()) {
						Object v = field.valueExp.getValue();
						if(!values.containsKey(field.getName()) || v !=null)
							values.put(field.getName(), v);
					}
				} catch (ManagedException e) {
					throw new DataException(e);
				}
			}
		}
		return drSource.lineVisited() > 0;
	}

	@Override
	public String getString(String fieldName) throws DataException {
		Object res = values.get(fieldName);
		if(res == null) return null;
		return res.toString();
	}

	@Override
	public Date getDate(String fieldName) throws DataException {
		return (Date) values.get(fieldName);
	}

	@Override
	public Double getDouble(String fieldName) throws DataException {
		Number res = (Number) values.get(fieldName);
		if(res == null) return null;
		return res.doubleValue();
	}

	@Override
	public boolean open() throws DataException {
		
		try {
			/*String drVariableName = DataManFactory.getDRVariableName(name);
			evaluator.getCurrentVariableContext().addVariable(drVariableName, DataReader.class, this);*/
			
			ObjectValue<XPOperand<?>> ovSource = config.getRequiredAttributAsObjectValue("source");
			
			String type = ovSource.getRequiredAttributAsString("type");
			
			DataManFactory dmf = dmFactories.get(type);
			
			if(dmf == null) throw new ManagedException(String.format("the type %s is unknown", type));
			
			drSource = dmf.getDataReader("source", ovSource, evaluator, variableContext, dmu);
			
			variableContext.addVariable("sourceDr", DataReader.class, drSource);
			
			ObjectValue<XPOperand<?>> fm = config.getAttributAsObjectValue("fields");
			
			this.defaultIfExp = fm.getAttribut("if");
			
			this.valueExp = fm.getAttribut("value");
			
			this.defaultValueExp = fm.getAttribut("default");
			
			this.typeExp = fm.getAttribut("type");
			
			String defaultType = null;
			
			if(this.typeExp != null) defaultType = this.typeExp.asString();
			if(defaultType == null) defaultType = "string";
			
			ObjectValue<XPOperand<?>> ovItems = fm.getRequiredAttributAsObjectValue("items");
			Map<String, Value<?,XPOperand<?>>> mpFields = ovItems.getValue();
			
			for(String fname : mpFields.keySet()) {
				Value<?, XPOperand<?>> vlField = mpFields.get(fname);
				
				String fieldType; Value<?, XPOperand<?>> ifExp = null; Value<?, XPOperand<?>> valueExp = null; ; Value<?, XPOperand<?>> defaultValueExp = null;
				BooleanValue<?> blField = vlField.asBooleanValue();
				
				if(blField == null) {
					StringValue<XPOperand<?>> sv = vlField.asStringValue(); 
					IntegerValue<XPOperand<?>> iv = vlField.asIntegerValue();
					BooleanValue<XPOperand<?>> bv = vlField.asBooleanValue();
					DecimalValue<XPOperand<?>> dcv = vlField.asDecimalValue();
					
					if(sv == null && iv == null && bv == null && dcv == null) {
						ObjectValue<XPOperand<?>> ovField = vlField.asRequiredObjectValue();
						
						fieldType = ovField.getAttributAsString("type");
						if(fieldType == null) fieldType = defaultType;
						
						ifExp = ovField.getAttribut("if");
						if(ifExp == null) ifExp = this.defaultIfExp;
						
						valueExp = ovField.getAttribut("value");
						if(valueExp == null) valueExp = this.valueExp;
						
						defaultValueExp = ovField.getAttribut("default");
						if(defaultValueExp == null) defaultValueExp = this.defaultValueExp;
					}
					else {
						fieldType = defaultType;
						valueExp = vlField;
					}
				}
				else {
					fieldType = defaultType;
					valueExp = this.valueExp;
				}
				if(valueExp == null) throw new DataException(String.format("No value expression defined for field %s", fname) );
				fields.put(fname, new Field(fname, fieldType, ifExp == null ? new BooleanValue<>(Boolean.TRUE) : ifExp, valueExp, defaultValueExp));
			}
			
			for(Field field : fields.values()) {
				if(field.defaultValueExp == null) continue;
				
				values.put(field.name, field.defaultValueExp.getValue());
			}
			
			for(DataReader<?> dr : dmu.getReaders().values()) {
				dr.open();
			}
			return drSource.open();
		} catch (ManagedException e) {
			throw new DataException(e);
		}
		
		
	}

	@Override
	public void close() throws DataException {
		if(drSource != null) try { drSource.close();} catch(DataException e) { e.printStackTrace();}
		
		for(DataReader<?> dr : dmu.getReaders().values()) {
			try { dr.close(); } catch(DataException e) { e.printStackTrace();}
		}
	}

	@Override
	public boolean isOpen() {
		return drSource != null;
	}

	@Override
	public RowToFieldDataReader cloneDM() throws DataException {
		return new RowToFieldDataReader(name, config, evaluator, variableContext, filesRepos, dataSources, defaultDataSource, dmu);
	}

	@Override
	public int lineVisited() {
		return drSource.lineVisited();
	}

	@Override
	public Integer getInteger(String fieldName) throws DataException {
		Number res = (Number) values.get(fieldName);
		if(res == null) return null;
		return res.intValue();
	}

}
