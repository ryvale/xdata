package com.exa.data.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exa.data.DataException;
import com.exa.data.Field;
import com.exa.data.StandardDataReaderBase;
import com.exa.data.config.utils.DMUtils;
import com.exa.expression.XPOperand;
import com.exa.lang.parsing.Computing;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;

public class SQLDataReader extends StandardDataReaderBase<Field> {
	
	public static final  Logger LOG = LoggerFactory.getLogger(SQLDataReader.class);
	
	public static boolean debugOn = false;
	
	protected String from = null;
	protected String criteria = null;
	protected String orderBy = null;
	protected String groupBy = null;
	
	private Map<String, String> fromTypes = new HashMap<>();
	
	protected Map<String, FieldManagerFactory> fieldsManagerFactories = new HashMap<>();
	
	private FieldManager fieldManager = null;
	
	private Connection connection = null;
	private ResultSet rs;
	private Statement sqlStmt;
	private Boolean dataRead = null;
	private ObjectValue<XPOperand<?>> config;
	
	private Boolean shouldNotOpen = null;
	
	private boolean valueFromCache = false;
	
	private boolean dataInBuffer = false;
	
	private Map<String, Object> cachedValues = new HashMap<>();
	
	protected int _lineVisited = 0;
	
	public SQLDataReader(String name, ObjectValue<XPOperand<?>> config, DMUtils dmu) throws DataException {
		super(name, dmu);
		
		this.config = config;
		
		fieldsManagerFactories.put("default", new FieldManagerFactory());
		fieldsManagerFactories.put("exa", new XAFieldManagerFactory());
	}
	
	@Override
	public boolean next() throws DataException {
		if(shouldNotOpen) return false;
		
		try {
			dataRead = rs.next();
			if(!dataRead) return dataInBuffer = false;
			++_lineVisited;
			
			valueFromCache = false;
			for(Field field : fields.values()) {
				if(field.isLongValue()) {
					cachedValues.put(field.getName(), getObject(field.getName()));
				}
			}
			valueFromCache = true;
			return dataInBuffer = true;
			
		} catch (SQLException e) {
			throw new DataException(e);
		}
		
	}

	public String getSQL() throws DataException {
		StringBuilder sql  = new StringBuilder();
		
		for(Field field :  fields.values()) {
			sql.append(", ").append(field.getExp() + " as " + field.getName());
		}
		
		if(sql.length() > 0) sql.delete(0, 2);
		
		sql.insert(0, "SELECT ");
		
		if(from != null) sql.append(" FROM ").append(from);
		
		if(criteria != null) sql.append(" WHERE ").append(criteria);
		
		if(groupBy != null) sql.append(" GROUP BY ").append(groupBy);
		
		if(orderBy != null) sql.append(" ORDER BY ").append(orderBy);
		
		return sql.toString();
	}

	@Override
	public String getString(String fieldName) throws DataException {
		try {
			if(valueFromCache && cachedValues.containsKey(fieldName)) {
				Object  ov = cachedValues.get(fieldName);
				if(ov == null) return null;
				return (String)ov;
			}
			
			String v = rs.getString(fieldName);
			if(v == null) return null;
			
			String fromType = fromTypes.get(fieldName);
			if(fromType == null) return v;
			
			if("html".equals(fromType)) 
				return v.toString().replaceAll("\\<.*?>","");
			
			return v;
		} catch (SQLException e) {
			throw new DataException(e);
		}
	}

	@Override
	public boolean open() throws DataException {
		if(debugOn) LOG.info(String.format("Opening '%s' Data Reader", name));
		
		try {
	
			from = config.getAttributAsString("from");
			
			if(from == null) {
				from = name;
			}
			
			ObjectValue<XPOperand<?>> fm = config.getAttributAsObjectValue("fields");
			
			String man;
			ObjectValue<XPOperand<?>> ovManager = fm.getAttributAsObjectValue("manager");
			if(ovManager == null) man = "default";
			else man = ovManager.getAttributAsString("_name", "default");
			
			FieldManagerFactory fmf = fieldsManagerFactories.get(man);
			if(fmf == null) throw new DataException(String.format("The field manager %s is unknown.", man));
			fieldManager = fmf.create(ovManager);
			
		 	Value<?, XPOperand<?>> vlFields = fm.getRequiredAttribut("items");
			
		 	ArrayValue<XPOperand<?>> avFields = vlFields.asArrayValue();
		 	if(avFields == null) {
		 		ObjectValue<XPOperand<?>> ovFields = fm.getRequiredAttributAsObjectValue("items");
				Map<String, Value<?,XPOperand<?>>> mpFields = ovFields.getValue();
				
				for(String fname : mpFields.keySet()) {
					if(Computing.PRTY_ENTITY.equals(fname)) continue;
					
					Value<?, XPOperand<?>> vlField = mpFields.get(fname);
					
					String exp, type;
					boolean longValue = false;
					BooleanValue<?> blField = vlField.asBooleanValue();
					if(blField == null) {
						StringValue<XPOperand<?>> sv = vlField.asStringValue();
						if(sv == null) {
							ObjectValue<XPOperand<?>> ov = vlField.asRequiredObjectValue();
							
							exp = ov.getAttributAsString("exp");
							type = ov.getAttributAsString("type", "string");
							Boolean v = ov.getAttributAsBoolean("longValue");
							
							String fromType = ov.getAttributAsString("from");
							fromTypes.put(fname, fromType);
							//longValue = ov.getAttributAsBoolean(fname);
							if(v != null) longValue = v;
						}
						else {
							exp = sv.getValue();
							type = "string";
						}
					}
					else {
						exp = null;
						type = "string";
					}
					
					Field field = new Field(fname, type);
					field.setExp(exp == null ? fieldManager.toSQL(fname) : exp);
					field.setLongValue(longValue);
					fields.put(fname, field);
				}
		 	}
		 	else {
		 		for(Value<?,XPOperand<?>> av : avFields.getValue()) {
					ObjectValue<XPOperand<?>> ov = av.asObjectValue();
					if(ov == null) throw new DataException(String.format("The array property fields item should object value."));
					
					String fname = ov.getRequiredAttributAsString("_name");
					
					String exp = ov.getAttributAsString("exp");
					
					String type = ov.getAttributAsString("type", "string");
					
					Field field = new Field(fname, type);
					field.setExp(exp == null ? fieldManager.toSQL(fname) : exp);
					
					fields.put(fname, field);
				}
		 	}
			
			criteria  = config.getAttributAsString("criteria");
			orderBy  = config.getAttributAsString("orderBy");
			groupBy  = config.getAttributAsString("groupBy");
						
			dmu.executeBeforeConnectionActions();
			
			Boolean shareConnection = config.getAttributAsBoolean("sharesConnection");
			
			boolean shouldCloseConnection = shareConnection != null && !shareConnection;
			
			dmu.setShouldCloseConnection(shouldCloseConnection);
			
			Value<?, XPOperand<?>> openCondition = config.getAttribut("openCondition");
			if(openCondition == null) shouldNotOpen = Boolean.FALSE;
			else {
				if(!"boolean".equals(openCondition.typeName())) throw new DataException(String.format("'openCondition' should a boolean expression in DataReader '%s'.", name));
				shouldNotOpen = !openCondition.asBoolean();
				if(shouldNotOpen) return false;
			}
			
			connection = dmu.getSqlConnection();
			
			String sql = getSQL();
			
			if(debugOn) System.out.println(sql);
			
			sqlStmt = connection.createStatement();
			rs = sqlStmt.executeQuery(sql);
			
			return true;
		}
		catch (ManagedException|SQLException e) {
			throw new DataException(e);
		}
	}

	@Override
	public void close() throws DataException {
		try {
			if(rs != null)
				try {
					
					rs.close();
					
					if(debugOn) System.out.println(String.format("Result set close for entity reader '%s'", name));
				} catch (SQLException e) {
					e.printStackTrace();
				}
			rs = null;
			dmu.releaseSqlConnection();
			dmu.clean();
			cachedValues.clear();
			connection = null;
			dataInBuffer = false;
			dataRead = null;
			
		} catch (ManagedException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public boolean isOpen() {
		return rs != null;
	}

	@Override
	public Date getDate(String fieldName) throws DataException {
		
		try {
			return rs.getDate(fieldName);
		} catch (SQLException e) {
			throw new DataException(e);
		}
	}

	@Override
	public Double getDouble(String fieldName) throws DataException {
		try {
			return rs.getDouble(fieldName);
		} catch (SQLException e) {
			throw new DataException(e);
		}
	}

	@Override
	public SQLDataReader cloneDM() throws DataException {
		SQLDataReader res = new SQLDataReader(name, config, dmu);
		if(isOpen()) res.open();
		return res;
		
	}

	@Override
	public int lineVisited() {
		return _lineVisited;
	}

	@Override
	public Integer getInteger(String fieldName) throws DataException {
		try {
			return rs.getInt(fieldName);
		} catch (SQLException e) {
			throw new DataException(e);
		}
	}

	@Override
	public boolean dataInBuffer() {
		return dataInBuffer;
	}

}
