package com.exa.data.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exa.data.DataException;
import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.DynamicField;
import com.exa.data.Field;
import com.exa.data.StandardDataWriterBase;
import com.exa.data.config.utils.BreakProperty;
import com.exa.data.config.utils.DMUtils;
import com.exa.data.config.utils.DataUserException;
import com.exa.data.sql.oracle.PLSQLDateFormatter;
import com.exa.data.sql.sqlserver.TSQLDateFormatter;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.lang.parsing.Computing;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;

public class SQLDataWriter extends StandardDataWriterBase<DynamicField> {
	private static final  Logger LOG = LoggerFactory.getLogger(SQLDataWriter.class);
	
	public static boolean debugOn = false;
	
	protected final static Map<String, DataFormatter<?>> formatters = new HashMap<>();
	protected final static Set<String> expTypes = new HashSet<>();
	
	final static DataFormatter<String> DF_STRING = new SQLStringFormatter();
	
	static {
		formatters.put("string-sql", DF_STRING);
		formatters.put("string-sql-server", DF_STRING);
		formatters.put("float-sqlserver", DF_STRING);
		formatters.put("string-oracle", DF_STRING);
		formatters.put("string-sql-oracle", DF_STRING);
		formatters.put("string-plsql", DF_STRING);
		formatters.put("string-tsql", DF_STRING);
		formatters.put("string-t-sql", DF_STRING);
		formatters.put("string-transact-sql", DF_STRING);
		
		DataFormatter<?> df = new SQLNumberFormatter();
		formatters.put("int-sql", df);
		formatters.put("int-sql-server", df);
		formatters.put("int-sqlserver", df);
		formatters.put("int-oracle", df);
		formatters.put("int-sql-oracle", df);
		formatters.put("int-plsql", df);
		formatters.put("int-tsql", df);
		formatters.put("int-t-sql", df);
		formatters.put("int-transact-sql", df);
		
		formatters.put("float-sql", df);
		formatters.put("float-sql-server", df);
		formatters.put("float-sqlserver", df);
		formatters.put("float-oracle", df);
		formatters.put("float-sql-oracle", df);
		formatters.put("float-plsql", df);
		formatters.put("float-tsql", df);
		formatters.put("float-t-sql", df);
		formatters.put("float-transact-sql", df);
		
		df = new PLSQLDateFormatter();
		formatters.put("datetime-oracle", df);
		formatters.put("datetime-sql-oracle", df);
		formatters.put("datetime-oracle", df);
		formatters.put("datetime-sql-oracle", df);
		formatters.put("datetime-plsql", df);
		formatters.put("datetime-pl-sql", df);
		
		formatters.put("date-oracle", df);
		formatters.put("date-sql-oracle", df);
		formatters.put("date-oracle", df);
		formatters.put("date-sql-oracle", df);
		formatters.put("date-plsql", df);
		formatters.put("date-pl-sql", df);
		
		
		df = new TSQLDateFormatter();
		formatters.put("datetime-sql-server", df);
		formatters.put("datetime-sqlserver", df);
		formatters.put("datetime-tsql", df);
		formatters.put("datetime-t-sql", df);
		formatters.put("datetime-transact-sql", df);
		
		formatters.put("date-sql-server", df);
		formatters.put("date-sqlserver", df);
		formatters.put("date-tsql", df);
		formatters.put("date-t-sql", df);
		formatters.put("date-transact-sql", df);
		
		
		expTypes.add("default");expTypes.add("reader");expTypes.add("value");expTypes.add("sql");expTypes.add("entire-sql");
	}
	
	//private DataSource dataSource;
	private Connection connection = null;
	private ObjectValue<XPOperand<?>> config;
	
	private boolean preventUpdate;
	
	private boolean preventInsertion;
	
	private FieldManager fieldManager = null;
	
	protected Map<String, FieldManagerFactory> fieldsManagerFactories = new HashMap<>();
	
	
	private Value<?, XPOperand<?>> vlTable;
	private Value<?, XPOperand<?>> fieldsItems;
	private Value<?, XPOperand<?>> vlWhere = null;
	private Value<?, XPOperand<?>> vlType;
	private Value<?, XPOperand<?>> vlNoInsert, vlNoUpdate;
	
	private List<BreakProperty> breakProperties = new ArrayList<>();
	
	private List<Value<?, XPOperand<?>>> lstKey = new ArrayList<>();

	public SQLDataWriter(String name, DataReader<?> drSource, ObjectValue<XPOperand<?>> config, DMUtils dmu, boolean preventInsertion, boolean preventUpdate) {
		super(name, drSource, dmu);
		
		this.preventInsertion = preventInsertion;
		this.preventUpdate = preventUpdate;
		
		this.config = config;
		
		this.drSource = drSource;
		
		fieldsManagerFactories.put("default", new FieldManagerFactory());
		fieldsManagerFactories.put("exa", new XAFieldManagerFactory());
	}

	@Override
	public int update(DataReader<?> dr) throws DataException {
		if(StandardDataWriterBase.debugOn) {
			LOG.info(String.format("START:update for writer '%s'", name));
		}
		try {
			
			String updateSQL = getSQL();
			
			if(updateSQL == null) return 0;
			
			if(debugOn) System.out.println(updateSQL);
			
			PreparedStatement ps = connection.prepareStatement(updateSQL);
			int res =  ps.executeUpdate();
			ps.close();
			
			if(StandardDataWriterBase.debugOn) {
				LOG.info(String.format("OK:update for writer '%s'", name));
			}
			return res;
			
			
		} catch (ManagedException|SQLException e) {
			LOG.info(String.format("FAIL:update for writer '%s' : '%s'", name, e.getMessage()));
		
			if(e instanceof DataException) throw (DataException)e;
			throw new DataException(e);
		}
		
	}
	
	private String getSQL() throws ManagedException, SQLException {
		if(StandardDataWriterBase.debugOn) {
			LOG.info(String.format("START:get sql for writer '%s'", name));
		}
		
		String table = vlTable.asRequiredString();
		VariableContext variableContext = dmu.getVc();
		if(StandardDataWriterBase.debugOn) {
			LOG.info(String.format("DATA: table = '%s' for '%s'", table, name));
		}
		
		boolean insertable = !vlNoInsert.asBoolean() && !preventInsertion;
		
		if(StandardDataWriterBase.debugOn) {
			LOG.info(String.format("DATA: insertable = '%s' for '%s'", insertable, name));
		}
		
		boolean updatable = !vlNoUpdate.asBoolean() && !preventUpdate && lstKey.size() != 0;
		
		if(StandardDataWriterBase.debugOn) {
			LOG.info(String.format("DATA: updatable = '%s' for '%s'", updatable, name));
		}
		
		if(insertable && updatable) {
			
			String where = getSQLWhere(table);
			
			if(StandardDataWriterBase.debugOn) {
				LOG.info(String.format("DATA: where = '%s' for '%s'", where, name));
			}
			
			DynamicField field = fields.get(lstKey.get(0).asRequiredString()) ;
			
			String recordSeekSql = "SELECT " + field.getVlName().asRequiredString() + " FROM " + table + " WHERE " + where;
			if(debugOn) System.out.println(recordSeekSql);
			
			if(StandardDataWriterBase.debugOn) {
				LOG.info(String.format("DATA: recordSeekSql = '%s' for '%s'", recordSeekSql, name));
			}
			
			PreparedStatement ps = connection.prepareStatement(recordSeekSql);
			ResultSet rs = ps.executeQuery();
			
			if(StandardDataWriterBase.debugOn) {
				LOG.info(String.format("EXEC: recordSeekSql query execution OK. for '%s'", name));
			}
			
			String updateSQL;
			if(rs.next()) {
				if(StandardDataWriterBase.debugOn) {
					LOG.info(String.format("STATE: first next return data for '%s'", name));
				}
				
				variableContext.assignOrDeclareVariable("updateMode", String.class, "update");
				if(mustBreak()) {
					ps.close();
					return null;
				}
				if(preventUpdate) {
					ps.close();
					return null;
				}
				updateSQL = getUpdateSQL(table, where);
			}
			else {
				if(StandardDataWriterBase.debugOn) {
					LOG.info(String.format("STATE: first next doesn't return data for '%s'", name));
				}
				variableContext.assignOrDeclareVariable("updateMode", String.class, "insert");
				if(mustBreak()) {
					ps.close();
					return null;
				}
				if(preventInsertion) {
					ps.close();
					LOG.warn(String.format("OK-KO:get sql return because of 'preventInsertion' for writer '%s'", name));
					return null;
				}
				updateSQL = getInsertSQL(table);
			}
			ps.close();
			
			if(StandardDataWriterBase.debugOn) {
				LOG.info(String.format("OK:get sql for writer '%s'", name));
			}
			return updateSQL;
		}
		
		if(insertable) {
			variableContext.assignOrDeclareVariable("updateMode", String.class, "insert");
			if(mustBreak()) return null;
			return getInsertSQL(table);
		}
		
		if(!updatable) return null;
		
		variableContext.assignOrDeclareVariable("updateMode", String.class, "update");
		if(mustBreak()) return null;
		
		String where = getSQLWhere(table);
		
		return getUpdateSQL(table, where);
		
	}
	
	
	private String getSQLWhere(String table) throws ManagedException {
		StringBuilder sbWhere = new StringBuilder();
		if(vlWhere != null) {
			sbWhere.append(vlWhere.asRequiredString());
		}
		
		String type = vlType.asRequiredString();
		
		StringBuilder sbKeyFields = new StringBuilder();
		
		for(Value<?, XPOperand<?>> vl : lstKey) {
			String keyField = vl.asRequiredString();
			DynamicField field = fields.get(keyField);
			
			String fieldName = field.getVlName().asRequiredString();
			//if(fieldForSelection == null) fieldForSelection = fieldName;
			
			if("reader".equals(field.getExpType())) {
				String ft = field.getType() + "-" + type;
				DataFormatter<?> dataf = formatters.get(ft);
				if(dataf == null) throw new ManagedException(String.format("No formatter provide for type '%s' for the field", ft, field.getName()));
				
				sbKeyFields.append(" AND ").
					append(fieldName).append(" = ").append(dataf.toSQLFormObject(drSource.getObject(field.getVlExp().asRequiredString())));
				continue;
			}
			
			if("value".equals(field.getExpType())) {
				String ft = field.getType() + "-" + type;
				DataFormatter<?> dataf = formatters.get(ft);
				
				sbKeyFields.append(" AND ").
					append(fieldName).append(" = ").append(dataf.toSQLFormObject(field.getVlExp().getValue()));
				continue;
			}
			
			if("sql".equals(field.getExpType())) {
				
				sbKeyFields.append(" AND ").append(field.getVlExp().asRequiredString());
				continue;
			}
			
			throw new ManagedException(String.format("Invalid expresssion type '%s' for field '%s'", field.getExpType(), field.getName()));
		}
		
		sbWhere.append(sbWhere.length() == 0 ? sbKeyFields.substring(4) : sbKeyFields.toString());
		
		return sbWhere.toString();
	}
	
	private boolean mustBreak() throws ManagedException {
		if(StandardDataWriterBase.debugOn) {
			LOG.info(String.format("START:mustBreak for writer '%s'", name));
		}
		
		for(BreakProperty bp : breakProperties) {
			if(bp.getVlCondition().asBoolean()) {
				if(bp.getVlThrowError() == null) {
					if(StandardDataWriterBase.debugOn) {
						LOG.info(String.format("OK:mustBreak with value '%s' for writer '%s'", true, name));
					}
					return true;
				}
				
				String errMess = bp.getVlThrowError().asString();
				
				String userMessage = bp.getVlUserMessage() == null ? null : bp.getVlUserMessage().asString();
				
				if(errMess == null) {
					if(StandardDataWriterBase.debugOn) {
						LOG.info(String.format("OK:mustBreak with value '%s' for writer '%s'", false, name));
					}
					return false;
				}
				
				if(StandardDataWriterBase.debugOn) {
					LOG.warn(String.format("OK-KO:mustBreak with message '%s' for writer '%s'", userMessage, name));
				}
				
				throw new  DataUserException(errMess, userMessage);
			}
		}
		
		if(StandardDataWriterBase.debugOn) {
			LOG.info(String.format("OK:mustBreak with value '%s' for writer '%s'", false, name));
		}
		return false;
	}

	@Override
	public DataWriter<DynamicField> cloneDM() throws DataException {

		return null;
	}

	
	public String getInsertSQL(String table) throws ManagedException {
		if(StandardDataWriterBase.debugOn) {
			LOG.info(String.format("START:getInsertSQL for writer '%s'", name));
		}
		StringBuilder sbFields = new StringBuilder();
		StringBuilder sbValues = new StringBuilder();
		
		String type = vlType.asRequiredString();
		
		for(DynamicField field : fields.values()) {
			if(!field.getVlCondition().asBoolean()) {
				if(StandardDataWriterBase.debugOn) {
					LOG.info(String.format("PROCESS:getInsertSQL skip field '%s' becasuse of condition for writer '%s'", field.getName(),  name));
				}
				continue;
			}
			
			if(StandardDataWriterBase.debugOn) {
				LOG.info(String.format("PROCESS:getInsertSQL start manage field '%s' with expression type '%S' in writer '%s'", field.getName(), field.getExpType(),  name));
			}
			
			String fn = field.getVlName().asRequiredString();
			sbFields.append(", ").append(fn);
			
			if(StandardDataWriterBase.debugOn) {
				LOG.info(String.format("PROCESS:getInsertSQL start managing field '%s' for writer '%s'", field.getName(),  name));
			}
			
			boolean fromString = field.getFrom() == null ? false : ("string".equals(field.getFrom().asString()) ? true : false);
			
			if(StandardDataWriterBase.debugOn && fromString) {
				LOG.info(String.format("DATA:getInsertSQL fromString = '%s' for writer '%s'", fromString,  name));
			}
			String fromFormat = field.getFromFormat() == null ? null : field.getFromFormat().asString();
			if(StandardDataWriterBase.debugOn && (fromFormat != null)) {
				LOG.info(String.format("DATA:getInsertSQL fromFormat = '%s' for writer '%s'", fromFormat,  name));
			}
			if("reader".equals(field.getExpType())) {
				String ft = field.getType() + "-" + type;
				DataFormatter<?> dataf = formatters.get(ft);
				
				if(dataf == null) {
					LOG.warn(String.format("KO:getInsertSQL no data formatter of type '%s' provided  in writer '%s'", ft, name));
					throw new ManagedException(String.format("No formatter provide for type '%s' for the field", ft, field.getName()));
				}
				
				if(StandardDataWriterBase.debugOn) {
					LOG.info(String.format("PROCESS:getInsertSQL adding field value '%s' for writer '%s'", field.getName(),  name));
				}
				
				sbValues.append(", ").append(fromString ? dataf.toSQLFromString(drSource.getString(field.getVlExp().asRequiredString()), fromFormat) : dataf.toSQLFormObject(drSource.getObject(field.getVlExp().asRequiredString())));
				continue;
			}
			
			if("value".equals(field.getExpType())) {
				String ft = field.getType() + "-" + type;
				DataFormatter<?> dataf = formatters.get(ft);
				
				if(dataf == null) {
					LOG.warn(String.format("KO:getInsertSQL no data formatter of type '%s' provided  in writer '%s'", ft, name));
					throw new ManagedException(String.format("No formatter provide for type '%s' for the field", ft, field.getName()));
				}
				
				
				sbValues.append(", ").append(fromString ? dataf.toSQLFromString(field.getVlExp().asString(), fromFormat) : dataf.toSQLFormObject(field.getVlExp().getValue()));
				continue;
			}
			
			if("sql".equals(field.getExpType())) {
				
				sbValues.append(", ").append(field.getVlExp().asRequiredString());
				continue;
			}
			
			LOG.warn(String.format("KO:getInsertSQL The expresssion type '%s' for field '%s' is not managed in this context in writer '%s'", field.getExpType(), field.getName(), name));
			throw new ManagedException(String.format("The expresssion type '%s' for field '%s' is not managed in this context", field.getExpType(), field.getName()));
		}
		
		if(StandardDataWriterBase.debugOn) {
			LOG.info(String.format("OK:getInsertSQL for writer '%s'", name));
		}
		return "INSERT INTO " + table + "(" + sbFields.substring(2)  + ") VALUES(" + sbValues.substring(2) + ")";
	}
	
	public String getUpdateSQL(String table, String where) throws ManagedException {
		
		StringBuilder sbFields = new StringBuilder();
		
		String type = vlType.asRequiredString();
		
		for(DynamicField field : fields.values()) {
			if(!field.getVlCondition().asBoolean()) continue;
			
			boolean fromString = field.getFrom() == null ? false : ("string".equals(field.getFrom().asString()) ? true : false);
			String fromFormat = field.getFromFormat() == null ? null : field.getFromFormat().asString();
			
			if("reader".equals(field.getExpType())) {
				String ft = field.getType() + "-" + type;
				DataFormatter<?> dataf = formatters.get(ft);
				if(dataf == null) throw new ManagedException(String.format("No formatter provide for type '%s' for the field", ft, field.getName()));
				
				sbFields.append(", ").
					append(field.getVlName().asRequiredString()).append(" = ").append(fromString ?  dataf.toSQLFromString(drSource.getString(field.getVlExp().asRequiredString()), fromFormat) : dataf.toSQLFormObject(drSource.getObject(field.getVlExp().asRequiredString())));
				continue;
			}
			
			if("value".equals(field.getExpType())) {
				String ft = field.getType() + "-" + type;
				DataFormatter<?> dataf = formatters.get(ft);
				
				sbFields.append(", ").
					append(field.getVlName().asRequiredString()).append(" = ").append(fromString ? dataf.toSQLFromString(field.getVlExp().asString(), fromFormat) : dataf.toSQLFormObject(field.getVlExp().getValue()));
				continue;
			}
			
			if("sql".equals(field.getExpType())) {
				
				sbFields.append(", ").
					append(field.getVlName().asRequiredString()).append(" = ").append(field.getVlExp().asRequiredString());
				continue;
			}
			
			if("entire-sql".equals(field.getExpType())) {
				
				sbFields.append(", ").append(field.getVlExp().asRequiredString());
				continue;
			}
		}
		
		return "UPDATE " + table + " SET " + sbFields.substring(2)  + " WHERE "  + where;
	}

	@Override
	public boolean open() throws DataException {
		vlTable = config.getAttribut("table");
		if(vlTable == null) vlTable = new StringValue<>(name);
		
		
		try {
			vlType = config.getRequiredAttribut("type");
			
			Value<?, XPOperand<?>> vlBreak = config.getAttribut("break");
			if(vlBreak == null) {
				vlBreak = new BooleanValue<>(Boolean.FALSE);
				breakProperties.add(new BreakProperty(new BooleanValue<>(Boolean.FALSE), null, null));
			}
			else {
				ArrayValue<XPOperand<?>> avBreak = vlBreak.asArrayValue();
				if(avBreak == null) {
					BreakProperty bp = BreakProperty.parseBreakItemConfig(vlBreak, name);
					breakProperties.add(bp);
				}
				else {
					List<Value<?, XPOperand<?>>> lstBreak = avBreak.getValue();
					for(Value<?, XPOperand<?>> vlBreakItem : lstBreak) {
						BreakProperty bp = BreakProperty.parseBreakItemConfig(vlBreakItem, name);
						breakProperties.add(bp);
					}
				}
			}
			
			
			vlNoInsert = config.getAttribut("noInsert");
			if(vlNoInsert == null) vlNoInsert = new BooleanValue<>(Boolean.FALSE);
			if(!"boolean".equals(vlNoInsert.typeName())) throw new DataException(String.format("The property 'noInsert' should be a boolean in data man '%S'", name));
			
			vlNoUpdate = config.getAttribut("noUpdate");
			if(vlNoUpdate == null) vlNoUpdate = new BooleanValue<>(Boolean.FALSE);
			if(!"boolean".equals(vlNoUpdate.typeName())) throw new DataException(String.format("The property 'noUpdate' should be a boolean in data man '%S'", name));
			
			ObjectValue<XPOperand<?>> fm = config.getAttributAsObjectValue("fields");
			
			Value<?, XPOperand<?>> vlKey = fm.getAttribut("key");
			
			if(vlKey != null) {
				ArrayValue<XPOperand<?>> avKey = vlKey.asArrayValue();
				if(avKey == null) {
					StringValue<XPOperand<?>> svKey = vlKey.asStringValue();
					if(svKey == null) {
						CalculableValue<?, XPOperand<?>> clKey = vlKey.asCalculableValue();
						if(clKey == null) throw new DataException(String.format("The Key attribut should be array, string or formula"));
					}
					lstKey.add(vlKey);
				}
				else {
					for(Value<?, XPOperand<?>> vl : avKey.getValue()) {
						lstKey.add(vl);
					}
				}
			}
			
			String man;
			ObjectValue<XPOperand<?>> ovManager = fm.getAttributAsObjectValue("manager");
			if(ovManager == null) man = "default";
			else man = ovManager.getAttributAsString("_name", "default");
			
			FieldManagerFactory fmf = fieldsManagerFactories.get(man);
			if(fmf == null) throw new DataException(String.format("The field manager %s is unknown.", man));
			fieldManager = fmf.create(ovManager);
			
			fieldsItems = fm.getRequiredAttribut("items");
			ArrayValue<XPOperand<?>> avFields = fieldsItems.asArrayValue();
			if(avFields == null) {
		 		ObjectValue<XPOperand<?>> ovFields = fieldsItems.asRequiredObjectValue();
				Map<String, Value<?,XPOperand<?>>> mpFields = ovFields.getValue();
				
				for(String fname : mpFields.keySet()) {
					if(Computing.PRTY_ENTITY.equals(fname)) continue;
					Value<?, XPOperand<?>> vlField = mpFields.get(fname);
					
					Value<?, XPOperand<?>> vlName, vlExp, vlCondition;
					String type, expType;
					BooleanValue<?> blField = vlField.asBooleanValue();
					
					Value<?, XPOperand<?>> from = null; Value<?, XPOperand<?>> fromFormat = null;
					if(blField == null) {
						StringValue<XPOperand<?>> sv = vlField.asStringValue();
						if(sv == null) {
							CalculableValue<?, XPOperand<?>> cl = vlField.asCalculableValue();
							
							if(cl == null) {
							
								ObjectValue<XPOperand<?>> ov = vlField.asRequiredObjectValue();
								
								vlName= ov.getAttribut("name");
								if(vlName == null) vlName = new StringValue<>(fieldManager.toSQL(fname));
								
								type = ov.getAttributAsString("type");
								vlExp = ov.getAttribut("exp");
								expType = ov.getAttributAsString("expType");
								
								from = ov.getAttribut("from");
								if(from != null)
									if(!"string".equals(from.typeName())) throw new DataException(String.format("Bad attribute type 'from' for field '%s' in data man '%s'. The value should be string", fname, name));
								fromFormat = ov.getAttribut("fromFormat");
								if(fromFormat != null)
									if(!"string".equals(fromFormat.typeName())) throw new DataException(String.format("Bad attribute type 'fromFormat' for field '%s' in data man '%s'. The value should be string", fname, name));
								
								if(from == null) {
									//For ascendant compatibility
									Value<?, XPOperand<?>> fromString = ov.getAttribut("fromString");
									if(fromString != null) {
										if(!"boolean".equals(fromString.typeName())) throw new DataException(String.format("Bad attribute type 'fromString' for field '%s' in data man '%s'. The value should be boolean", fname, name));
										
										if(fromString.asRequiredBoolean()) from = new StringValue<>("string");
									}
									
									if(fromFormat != null) from = new StringValue<>("string");
									
								}
										
								if(type == null) {
									if(expType == null) {
										
										if(vlExp == null) {
											Field f = drSource.getField(fname);
											if(f == null) throw new DataException(String.format("Unable to infer the type  for field '%s' when 'exp', 'expType' properties is not set in data man '%s'", fname, name));
											
											expType = "reader";
											vlExp = new StringValue<>(fname);
											type = f.getType();
										}
										else {
											CalculableValue<?, XPOperand<?>> clexp = vlExp.asCalculableValue();
											if(clexp == null) {
												if(vlExp.asStringValue() == null) throw new DataException(String.format("Unable to infer the type  for field '%s' when 'type', 'expType' properties is not set and 'exp' is not a string value in data man '%s'", fname, name));
												String expFieldName = vlExp.asRequiredString();
												Field f = drSource.getField(expFieldName);
												if(f == null) throw new DataException(String.format("Unable to infer the type  for field '%s' when 'type', 'expType' properties is not set and 'exp' '%s' is not a source field in data man '%s'", fname, expFieldName, name));
												
												expType = "reader";
												type = f.getType();
											} else {
												expType = "value";
												type = vlExp.typeName();
											}
										}
									}
									else {
										if("value".equals(expType)) {
											if(vlExp == null) throw new DataException(String.format("Unable to infer the type  for field '%s' when 'expType' = 'value' and 'exp' is not set in data man '%s'", fname, name));
											type = vlExp.typeName();
										}
										else if("reader".equals(expType)) {
											if(vlExp == null) {
												Field f = drSource.getField(fname);
												if(f == null) throw new DataException(String.format("Unable to infer the type  for field '%s' because '%s' doesn't exist in source in data man '%s'", fname, fname, name));
												
												vlExp = new StringValue<>(fname);
												type = f.getType();
											}
											else {
												if(vlExp.asStringValue() == null) throw new DataException(String.format("Unable to infer the type  for field '%s' when 'expType' = 'reader' and 'exp' is not a string value in data man '%s'", fname, name));
												
												String expFname = vlExp.asRequiredString();
												Field f = drSource.getField(expFname);
												if(f == null) throw new DataException(String.format("Unable to infer the type  for field '%s' when 'expType' = 'reader' and 'exp' value (%s) is not a source field in data man '%s'", fname, expFname, name));

												type = f.getType();
											}
										}
										else if("sql".equals(expType) || "entire-sql".equals(expType)) {
											if(vlExp == null) throw new DataException(String.format("'exp' sould not be null when 'expType' = 'sql' for field '%s' in data man '%s'", fname, name));
											
											if(!"string".equals(vlExp.typeName())) throw new DataException(String.format("'exp' sould be a string when 'expType' = 'sql' for field '%s' in data man '%s'", fname, name));
										}
									}
								}
								else if(expType == null) {
									if(vlExp == null) {
										Field f = drSource.getField(fname);
										if(f == null) throw new DataException(String.format("Unable to infer the type  for field '%s' when 'exp', 'expType' properties is not set in data man '%s'", fname, name));
										
										expType = "reader";
										vlExp = new StringValue<>(fname);
										
										String vfrom = from == null ? null : from.asString();
										
										if(typeMismatch("string".equals(vfrom) ? "string" : type, f.getType())) 
											throw new DataException(String.format("Type imcompatible for field '%s' (%s != %s) in data man %S", fname, type, f.getType(), name));
									}
									else {
										CalculableValue<?, XPOperand<?>> clexp = vlExp.asCalculableValue();
										if(clexp == null) {
											
											if(vlExp.asStringValue() == null)
												throw new DataException(String.format("Unable to infer the type  for field '%s' when 'expType' = 'reader' and 'exp' is not a string value in data man '%s'", fname, name));
											
											String expFname = vlExp.asRequiredString();
											Field f = drSource.getField(expFname);
											if(f == null) throw new DataException(String.format("Unable to infer the type  for field '%s' when 'expType' = 'reader' and 'exp' value (%s) is not a source field in data man '%s'", fname, expFname, name));
											
											expType = "reader";
											
											String vfrom = from == null ? null : from.asString();
											
											if(typeMismatch("string".equals(vfrom) ? "string" : type, f.getType())) 
												throw new DataException(String.format("Type imcompatible for field '%s' (%s != %s) in data man %S", fname, type, f.getType(), name));
										}
										else {
											expType = "reader";
											
											if(!"string".equals(vlExp.typeName())) throw new DataException(String.format("'exp' should be a string when 'expType' = 'reader' for field '%s' in data man '%s'", fname, name));
										}
									}
								}
								else {
									if("value".equals(expType)) {
										if(vlExp == null) throw new DataException(String.format("Unable to infer the type  for field '%s' when 'expType' = 'value' and 'exp' is not set in data man '%s'", fname, name));
				
										String vfrom = from == null ? null : from.asString();
										
										if(typeMismatch("string".equals(vfrom) ? "string" : type,  vlExp.typeName())) 
											throw new DataException(String.format("Type imcompatible for field '%s' (%s != %s) in data man %S", fname, type, vlExp.typeName(), name));
									}
									else if("reader".equals(expType)) {
										if(vlExp == null) vlExp = new StringValue<>(fname);
									}
									else if("sql".equals(expType) || "entire-sql".equals(expType)) {
										if(vlExp == null) throw new DataException(String.format("'exp' sould not be null when 'expType' = 'sql' for field '%s' in data man '%s'", fname, name));
										if(!"string".equals(vlExp.typeName())) throw new DataException(String.format("'exp' sould be a string when 'expType' = 'sql' for field '%s' in data man '%s'", fname, name));
									}
								}
								
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
								vlName = new StringValue<>(fieldManager.toSQL(fname));
								vlExp = cl;
								type = cl.typeName();
								expType = "value";
								//from = new BooleanValue<>(Boolean.FALSE);
								vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
							}
						}
						else {
							Field f = drSource.getField(sv.getValue());
							if(f == null) throw new DataException(String.format("Unable to infer the type  for field '%s' because '%s' doesn't exist in source in data man '%s'", fname, fname, name));
							
							vlName = new StringValue<>(fieldManager.toSQL(fname));
							vlExp = sv;
							expType = "reader";
							
							//from = new BooleanValue<>(Boolean.FALSE);
							
							type = f.getType();
							vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
						}
					}
					else {
						Field f = drSource.getField(fname);
						if(f == null) throw new DataException(String.format("Unable to infer the type  for field '%s' because '%s' doesn't exist in source in data man '%s'", fname, fname, name));
						vlName = new StringValue<>(fieldManager.toSQL(fname));
						vlExp = new StringValue<>(fname);
						expType = "reader";
						//from = new BooleanValue<>(Boolean.FALSE);
						
						type = f.getType();
						vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
					}
					
					if(!expTypes.contains(expType)) throw new DataException(String.format("Invalid expresssion type '%s' for field '%s'", expType, fname));
					
					
					if("default".equals(expType)) expType = "reader";
					
					DynamicField field = new DynamicField(fname, type, expType);
					field.setVlExp(vlExp);
					field.setVlCondition(vlCondition);
					field.setVlName(vlName);
					field.setFrom(from);
					field.setFromFormat(fromFormat);
					
					fields.put(fname, field);
				}
		 	}
		 	else {
		 		//TODO
		 	}
			vlWhere = config.getAttribut("criteria");
			
			dmu.executeBeforeConnectionActions();
			
			Boolean shareConnection = config.getAttributAsBoolean("sharesConnection");
			
			boolean shouldCloseConnection = shareConnection != null && !shareConnection;
			
			dmu.setShouldCloseConnection(shouldCloseConnection);
			
			connection = dmu.getSqlConnection();
		}
		catch(ManagedException|SQLException e) {
			//if(connection != null) try { connection.close(); } catch (Exception e2) { e2.printStackTrace(); }
			throw new DataException(e);
		}
		
		return true;
	}
	
	private boolean typeMismatch(String t1, String t2) {
		if(t1.equals("int") || t1.equals("integer")) {
			if(!(t2.equals("int") || t2.equals("integer"))) return true;
			return false;
		}
		
		if(t1.equals("float") || t1.equals("double") || t1.equals("decimal")) {
			if(!(t2.equals("float") || t2.equals("double") || t2.equals("decimal"))) return true;
			return false;
		}
		
		if(t1.equals("date") || t1.equals("datetime") || t1.equals("time")) {
			if(!(t2.equals("date") || t2.equals("datetime") || t2.equals("time"))) return true;
			return false;
		}
		
		return !t1.equals(t2);
	}
	
	

	@Override
	public void close() throws DataException {
		
		lstKey.clear();
		breakProperties.clear();
		
		if(connection != null) {
			try {
				dmu.releaseSqlConnection();
			} catch (ManagedException e) {
				e.printStackTrace();
			}
			//try { connection.close(); if(debugOn) LOG.info(String.format("connexion closed for '%s' Data writer-", name)  + this.hashCode()); } catch (SQLException e) { e.printStackTrace(); }
			dmu.clean();
			connection = null;
		}
		
	}

	@Override
	public boolean isOpen() {
		return connection != null;
	}

}
