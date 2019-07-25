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
import com.exa.data.StandardDataWriterBase;
import com.exa.data.config.utils.BreakProperty;
import com.exa.data.config.utils.DMUtils;
import com.exa.data.config.utils.DataUserException;
import com.exa.data.sql.oracle.PLSQLDateFormatter;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;

public class SQLDataWriter extends StandardDataWriterBase<DynamicField> {
	public static final  Logger LOG = LoggerFactory.getLogger(SQLDataWriter.class);
	
	public static boolean debugOn = false;
	
	protected final static Map<String, DataFormatter<?>> formatters = new HashMap<>();
	protected final static Set<String> expTypes = new HashSet<>();
	
	final static DataFormatter<String> DF_STRING = new SQLStringFormatter();
	
	static {
		formatters.put("string-sql", DF_STRING);
		formatters.put("string-sql-server", DF_STRING);
		formatters.put("string-oracle", DF_STRING);
		formatters.put("string-sql-oracle", DF_STRING);
		formatters.put("string-plsql", DF_STRING);
		formatters.put("string-tsql", DF_STRING);
		formatters.put("string-t-sql", DF_STRING);
		formatters.put("string-transact-sql", DF_STRING);
		
		DataFormatter<?> df = new SQLNumberFormatter();
		formatters.put("int-sql", df);
		formatters.put("int-sql-server", df);
		formatters.put("int-oracle", df);
		formatters.put("int-sql-oracle", df);
		formatters.put("int-plsql", df);
		formatters.put("int-tsql", df);
		formatters.put("int-t-sql", df);
		formatters.put("int-transact-sql", df);
		
		formatters.put("float-sql", df);
		formatters.put("float-sql-server", df);
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
		
		try {
			
			String updateSQL = getSQL();
			
			if(updateSQL == null) return 0;
			
			if(debugOn) System.out.println(updateSQL);
			PreparedStatement ps = connection.prepareStatement(updateSQL);
			return ps.executeUpdate();
			
			
		} catch (ManagedException|SQLException e) {
			if(e instanceof DataException) throw (DataException)e;
			throw new DataException(e);
		}
		
	}
	
	private String getSQL() throws ManagedException, SQLException {
		String table = vlTable.asRequiredString();
		VariableContext variableContext = dmu.getVc();
		
		if(lstKey.size() == 0) {
			if(preventInsertion) return null;
			variableContext.assignOrDeclareVariable("updateMode", String.class, "insert");
			if(mustBreak()) return null;
			
			String sql = getInsertSQL(table);
			return sql;
		}
		
		if(!preventInsertion && !preventUpdate) {
			
			String where = getSQLWhere(table);
			
			DynamicField field = fields.get(lstKey.get(0).asRequiredString()) ;
			
			String recordSeekSql = "SELECT " + field.getVlName().asRequiredString() + " FROM " + table + " WHERE " + where;
			PreparedStatement ps = connection.prepareStatement(recordSeekSql);
			ResultSet rs = ps.executeQuery();
			
			String updateSQL;
			if(rs.next()) {
				variableContext.assignOrDeclareVariable("updateMode", String.class, "update");
				if(mustBreak()) return null;
				if(preventUpdate) return null;
				updateSQL = getUpdateSQL(table, where);
			}
			else {
				variableContext.assignOrDeclareVariable("updateMode", String.class, "insert");
				if(mustBreak()) return null;
				if(preventInsertion) return null;
				updateSQL = getInsertSQL(table);
			}
			ps.close();
			
			return updateSQL;
		}
		
		if(preventUpdate) {
			variableContext.assignOrDeclareVariable("updateMode", String.class, "insert");
			if(mustBreak()) return null;
			return getInsertSQL(table);
		}
		
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
				
				sbKeyFields.append("AND ").
					append(fieldName).append(" = ").append(dataf.toSQLFormObject(drSource.getObject(field.getVlExp().asRequiredString())));
				continue;
			}
			
			if("value".equals(field.getExpType())) {
				String ft = field.getType() + "-" + type;
				DataFormatter<?> dataf = formatters.get(ft);
				
				sbKeyFields.append(", ").
					append(fieldName).append(" = ").append(dataf.toSQLFormObject(field.getVlExp().getValue()));
				continue;
			}
			
			if("sql".equals(field.getExpType())) {
				
				sbKeyFields.append(", ").append(field.getVlExp().asRequiredString());
				continue;
			}
			
			throw new ManagedException(String.format("Invalid expresssion type '%' for field '%s'", field.getExpType(), field.getName()));
		}
		
		sbWhere.append(sbWhere.length() == 0 ? sbKeyFields.substring(4) : sbKeyFields.toString());
		
		return sbWhere.toString();
	}
	
	private boolean mustBreak() throws ManagedException {
		for(BreakProperty bp : breakProperties) {
			if(bp.getVlCondition().asBoolean()) {
				if(bp.getVlThrowError() == null) return true;
				
				String errMess = bp.getVlThrowError().asString();
				
				String userMessage = bp.getVlUserMessage() == null ? null : bp.getVlUserMessage().asString();
				
				if(errMess == null) return false;
				
				throw new  DataUserException(errMess, userMessage);
			}
		}
		
		return false;
	}

	@Override
	public DataWriter<DynamicField> cloneDM() throws DataException {

		return null;
	}

	
	public String getInsertSQL(String table) throws ManagedException {
		StringBuilder sbFields = new StringBuilder();
		StringBuilder sbValues = new StringBuilder();
		
		String type = vlType.asRequiredString();
		
		for(DynamicField field : fields.values()) {
			if(!field.getVlCondition().asBoolean()) continue;
			
			sbFields.append(", ").append(field.getVlName().asRequiredString());
			if("reader".equals(field.getExpType())) {
				String ft = field.getType() + "-" + type;
				DataFormatter<?> dataf = formatters.get(ft);
				if(dataf == null) throw new ManagedException(String.format("No formatter provide for type '%s' for the field", ft, field.getName()));
				
				sbValues.append(", ").append(dataf.toSQLFormObject(drSource.getObject(field.getVlExp().asRequiredString())));
				continue;
			}
			
			if("value".equals(field.getExpType())) {
				String ft = field.getType() + "-" + type;
				DataFormatter<?> dataf = formatters.get(ft);
				sbValues.append(", ").append(dataf.toSQLFormObject(field.getVlExp().getValue()));
				continue;
			}
			
			if("sql".equals(field.getExpType())) {
				
				sbValues.append(", ").append(field.getVlExp().asRequiredString());
				continue;
			}
			
			throw new ManagedException(String.format("The expresssion type '%' for field '%s' is not managed in this context", field.getExpType(), field.getName()));
		}
		
		return "INSERT INTO " + table + "(" + sbFields.substring(2)  + ") VALUES(" + sbValues.substring(2) + ")";
	}
	
	public String getUpdateSQL(String table, String where) throws ManagedException {
		
		StringBuilder sbFields = new StringBuilder();
		
		String type = vlType.asRequiredString();
		
		for(DynamicField field : fields.values()) {
			if(!field.getVlCondition().asBoolean()) continue;
			
			if("reader".equals(field.getExpType())) {
				String ft = field.getType() + "-" + type;
				DataFormatter<?> dataf = formatters.get(ft);
				if(dataf == null) throw new ManagedException(String.format("No formatter provide for type '%s' for the field", ft, field.getName()));
				
				sbFields.append(", ").
					append(field.getVlName().asRequiredString()).append(" = ").append(dataf.toSQLFormObject(drSource.getObject(field.getVlExp().asRequiredString())));
				continue;
			}
			
			if("value".equals(field.getExpType())) {
				String ft = field.getType() + "-" + type;
				DataFormatter<?> dataf = formatters.get(ft);
				
				sbFields.append(", ").
					append(field.getVlName().asRequiredString()).append(" = ").append(dataf.toSQLFormObject(field.getVlExp().getValue()));
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
					Value<?, XPOperand<?>> vlField = mpFields.get(fname);
					
					Value<?, XPOperand<?>> vlName, vlExp, vlCondition;
					String type, expType;
					BooleanValue<?> blField = vlField.asBooleanValue();
					if(blField == null) {
						StringValue<XPOperand<?>> sv = vlField.asStringValue();
						if(sv == null) {
							ObjectValue<XPOperand<?>> ov = vlField.asRequiredObjectValue();
							
							vlName= ov.getAttribut("name");
							if(vlName == null) vlName = new StringValue<>(fieldManager.toSQL(fname));
							vlExp = ov.getRequiredAttribut("exp");
							type = ov.getAttributAsString("type");
							expType = ov.getAttributAsString("expType", "reader");
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
							vlExp = sv;
							expType = "reader";
							type = sv.typeName();
							vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
						}
					}
					else {
						vlName = new StringValue<>(fieldManager.toSQL(fname));
						vlExp = new StringValue<>(fname);
						expType = "reader";
						type = "string";
						vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
					}
					
					if(!expTypes.contains(expType)) throw new DataException(String.format("Invalid expresssion type '%' for field '%s'", expType, fname));
					
					if("value".equals(expType)) {
						String et = vlExp.typeName();
						if(type == null) type = et;
						else {
							if(!type.equals(et)) {
								throw new DataException(String.format("Field '%s' type '%s' mismatch with the expression type %s", fname, type, et));
							}
						}
					}
					else if(type == null) type= "string";
					
					if("default".equals(expType)) expType = "reader";
					
					DynamicField field = new DynamicField(fname, type, expType);
					field.setVlExp(vlExp);
					field.setVlCondition(vlCondition);
					field.setVlName(vlName);
					
					fields.put(fname, field);
				}
		 	}
		 	else {
		 		//TODO
		 	}
			vlWhere = config.getAttribut("criteria");
			
			dmu.executeBeforeConnectionActions();
			
			Boolean shareConnection = config.getAttributAsBoolean("sharesConnection");
			
			boolean shouldCloseConnection = shareConnection == null || !shareConnection;
			
			dmu.setShouldCloseConnection(shouldCloseConnection);
			
			connection = dmu.getSqlConnection();
		}
		catch(ManagedException|SQLException e) {
			//if(connection != null) try { connection.close(); } catch (Exception e2) { e2.printStackTrace(); }
			throw new DataException(e);
		}
		
		return true;
	}
	
	

	@Override
	public void close() throws DataException {
		
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
