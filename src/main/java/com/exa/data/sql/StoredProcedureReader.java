package com.exa.data.sql;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.exa.data.DataException;
import com.exa.data.DynamicField;
import com.exa.data.StandardDataReaderBase;
import com.exa.data.config.utils.DMUtils;
import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;

public class StoredProcedureReader  extends StandardDataReaderBase<DynamicField> {
	
	protected final static Set<String> expTypes = new HashSet<>();
	
	static {
		expTypes.add("default");expTypes.add("value");expTypes.add("sp-out");expTypes.add("sp-return");
	}
	
	class Param {
		private String name;
		
		private String type;
		
		private String ioType;
		
		private Value<?, XPOperand<?>> value;

		public Param(String name, String type, String ioType, Value<?, XPOperand<?>> value) {
			super();
			this.name = name;
			this.type = type;
			this.ioType = ioType;
			this.value = value;
		}

		public String getName() {
			return name;
		}

		public String getType() {
			return type;
		}

		public String getIoType() {
			return ioType;
		}

		public Value<?, XPOperand<?>> getValue() {
			return value;
		}

		public void setValue(Value<?, XPOperand<?>> value) {
			this.value = value;
		}
		
		
	}
	
	private Value<?, XPOperand<?>> vlTable;
	private Value<?, XPOperand<?>> fieldsItems;
	
	private List<Param> params = new ArrayList<>();
	
	private ObjectValue<XPOperand<?>> config;

	protected int _lineVisited = 0;
	
	private boolean dataInBuffer = false;
	
	private Connection connection = null;
	
	private CallableStatement spStatement;

	public StoredProcedureReader(String name, ObjectValue<XPOperand<?>> config, DMUtils dmu) {
		super(name, dmu);
		
		this.config = config;
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
		
		if(field == null) throw new DataException(String.format("No field named %s in reader %s", fieldName, name));
		
		if(!"string".equals(field.getType())) throw new DataException(String.format("The field %s is not string", name));
		
		try {
			if("sp-out".equals(field.getExpType())) return spStatement.getString(field.getVlExp().asString());
			
			if("value".equals(field.getExpType())) { 
				if(!"string".equals(field.getVlExp().typeName())) throw new DataException(String.format("The field %s is not a string in reader %s", fieldName, name));
				return field.getVlExp().asString();
			}
			
		} catch (ManagedException| SQLException e) {
			throw new DataException(e);
		}
		
		throw new DataException(String.format("The field expression type %s for field %s is not yet managed in reader %s", field.getExpType(), fieldName, name));
	}

	@Override
	public Integer getInteger(String fieldName) throws DataException {
		DynamicField field = fields.get(fieldName);
		
		if(field == null) throw new DataException(String.format("No field named %s in reader %s", fieldName, name));
		
		if(!"int".equals(field.getType())) throw new DataException(String.format("The field %s is not an int in reader %s", fieldName, name));
		
		try {
			if("sp-out".equals(field.getExpType())) return spStatement.getInt(field.getVlExp().asString());
			
			if("value".equals(field.getExpType())) { 
				if(!"int".equals(field.getVlExp().typeName())) throw new DataException(String.format("The field %s is not an int in reader %s", fieldName, name));
				return field.getVlExp().asInteger();
			}
			
			
		} catch (SQLException | ManagedException e) {
			throw new DataException(e);
		}
		
		throw new DataException(String.format("The field expression type %s for field %s is not yet managed in reader %s", field.getExpType(), fieldName, name));
	}

	@Override
	public Date getDate(String fieldName) throws DataException {
		DynamicField field = fields.get(fieldName);
		
		if(field == null) throw new DataException(String.format("No field named %s in reader %s", fieldName, name));
		
		if(!("date".equals(field.getType()) || "datetime".equals(field.getType()) || "time".equals(field.getType()))) throw new DataException(String.format("The field %s is not a date", name));
		
		try {
			if("sp-out".equals(field.getExpType())) return spStatement.getDate(field.getName());
			
			/*if("value".equals(field.getExpType())) { 
				if(!"int".equals(field.getVlExp().typeName())) throw new DataException(String.format("The field %s is not an int in reader %s", fieldName, name));
				return field.getVlExp().asDecimalValue();
			}*/
			
		} catch (SQLException e) {
			throw new DataException(e);
		}
		
		throw new DataException(String.format("The field expression type %s for field %s is not yet managed in reader %s", field.getExpType(), fieldName, name));
	}

	@Override
	public Double getDouble(String fieldName) throws DataException {
		DynamicField field = fields.get(fieldName);
		
		if(field == null) throw new DataException(String.format("No field named %s in reader %s", fieldName, name));
		
		if(!("float".equals(field.getType()) || "decimal".equals(field.getType()) || "double".equals(field.getType()))) throw new DataException(String.format("The field %s is not a float", name));
		
		try {
			if("sp-out".equals(field.getExpType())) return spStatement.getDouble(field.getName());
			
			if("value".equals(field.getExpType())) { 
				if(!("decimal".equals(field.getVlExp().typeName()) || "float".equals(field.getVlExp().typeName()) || "double".equals(field.getVlExp().typeName()))) throw new DataException(String.format("The field %s is not a float in reader %s", fieldName, name));
				return field.getVlExp().asDecimalValue().getValue();
			}
			
		} catch (SQLException e) {
			throw new DataException(e);
		}
		
		throw new DataException(String.format("The field expression type %s for field %s is not yet managed in reader %s", field.getExpType(), fieldName, name));
	}

	@Override
	public boolean open() throws DataException {
		vlTable = config.getAttribut("table");
		if(vlTable == null) vlTable = new StringValue<>(name);
		
		try {
			if(config.containsAttribut("params")) {
				ObjectValue<XPOperand<?>> ovParams = config.getAttributAsObjectValue("params");
				if(ovParams == null) throw new DataException(String.format("The property '%s' of the entity %s should be an object", "params", name));
								
				Map<String, Value<?, XPOperand<?>>> mpParams = ovParams.getValue();
				for(String paramName : mpParams.keySet()) {
					Value<?, XPOperand<?>> vlParam = mpParams.get(paramName);
					String paramTypeName = vlParam.typeName();
					
					String name; String type; String ioType; Value<?, XPOperand<?>> value = null;
					if("object".equals(paramTypeName)) {
						ObjectValue<XPOperand<?>> ovParam = vlParam.asObjectValue();
						
						name = ovParam.containsAttribut("_name") ? ovParam.getAttributAsString("_name") : paramName;
						type = ovParam.getAttributAsString("type", "string");
						ioType = ovParam.getAttributAsString("ioType", "out");
						
						if("in".equals(ioType)) value = ovParam.getRequiredAttribut("value");
					}
					else if("string".equals(paramTypeName) || "float".equals(paramTypeName) || "decimal".equals(paramTypeName)  || "int".equals(paramTypeName)) {
						name =  paramName;
						type = paramTypeName;
						ioType = "in";
						value = vlParam;
					}
					else if("boolean".equals(paramTypeName)) {
						name =  paramName;
						type =  "string";
						ioType = "out";
					}
					else throw new DataException(String.format("Non managed type '%s' in params for reader", paramTypeName, this.name));
					
					params.add(new Param(name, type, ioType, value));
				}
			}
			
			ObjectValue<XPOperand<?>> fm = config.getAttributAsObjectValue("fields");
			fieldsItems = fm.getRequiredAttribut("items");
			ArrayValue<XPOperand<?>> avFields = fieldsItems.asArrayValue();
			
			if(avFields == null) {
				ObjectValue<XPOperand<?>> ovFields = fieldsItems.asRequiredObjectValue();
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
							expType = ov.getAttributAsString("expType", "sp-out");
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
							expType = "sp-out";
							type = "string";
							vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
						}
					}
					else {
						vlExp = new StringValue<>(fname);
						expType = "sp-out";
						type = "string";
						vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
					}
					
					if(!expTypes.contains(expType)) throw new DataException(String.format("Invalid expresssion type '%' for field '%s'", expType, fname));
					
					if("value".equals(expType) && !"string".equals(type)) throw new DataException(String.format("For the expression type 'value' the field type should be instead of %s for field %s", type, fname));
					
					if("default".equals(expType)) expType = "sp-out";
					
					DynamicField field = new DynamicField(fname, type, expType);
					field.setVlExp(vlExp);
					field.setVlCondition(vlCondition);
					
					fields.put(fname, field);
				}
			}
			else {
			}
			
			StringBuilder sbSql = new StringBuilder();
			
			for (int i = 0; i < params.size(); i++) {
				sbSql.append(", ?");
			}
			if(sbSql.length() > 0) sbSql.delete(0, 2);
			
			dmu.executeBeforeConnectionActions();
			
			Boolean shareConnection = config.getAttributAsBoolean("sharesConnection");
			
			shouldCloseConnection = shareConnection == null || !shareConnection;
			
			dmu.setShouldCloseConnection(shouldCloseConnection);
			
			connection = dmu.getSqlConnection();// dataSource.getConnection();
			//if(SQLDataReader.debugOn) System.out.println(String.format("connexion open for '%s' Data reader-", name)  + this.hashCode());
			String sql = "{call " + vlTable.asRequiredString() + "(" + sbSql + ")}";
			if(SQLDataReader.debugOn) System.out.println(sql);
			spStatement = connection.prepareCall(sql);
			
			for(Param p : params) {
				
				if("in".equals(p.getIoType())) {
					if("int".equals(p.type)) spStatement.setInt(p.name, p.getValue().asInteger());
					else if("float".equals(p.type) || "decimal".equals(p.type)) spStatement.setDouble(p.name, p.getValue().asDecimalValue().getValue());
					else spStatement.setString(p.name, p.getValue().asString());
					
					continue;
				}
				
				if("int".equals(p.type)) spStatement.registerOutParameter(p.name, Types.INTEGER);
				else if("float".equals(p.type) || "decimal".equals(p.type)) spStatement.registerOutParameter(p.name, Types.DOUBLE);
				else spStatement.registerOutParameter(p.name, Types.VARCHAR);
			}
			
			spStatement.executeUpdate();
		} 
		catch(ManagedException | SQLException e) {
			if(connection != null) try { connection.close(); } catch (Exception e2) { e2.printStackTrace(); }
			throw new DataException(e);
		}
		return true;
	}

	@Override
	public void close() throws DataException {
		
		params.clear();
		if(connection != null)
			try {
				dmu.releaseSqlConnection(); dmu.clean();
			} catch (ManagedException e) {
				e.printStackTrace();
			}
		spStatement = null;
		connection = null;
	}

	@Override
	public int lineVisited() { return _lineVisited; }

	@Override
	public boolean isOpen() {
		return connection != null;
	}

	@Override
	public StoredProcedureReader cloneDM() throws DataException {
		return new StoredProcedureReader(name, config, dmu);
	}

	@Override
	public boolean dataInBuffer() {
		return dataInBuffer;
	}

	

}
