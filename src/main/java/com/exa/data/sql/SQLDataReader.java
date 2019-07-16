package com.exa.data.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.exa.data.DataException;
import com.exa.data.DataReader;
import com.exa.data.Field;
import com.exa.data.StandardDataReaderBase;
import com.exa.data.config.utils.DMUtils;
import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;

public class SQLDataReader extends StandardDataReaderBase<Field> {
	
	protected String from = null;
	protected String criteria = null;
	protected String orderBy = null;
	protected String groupBy = null;
	
	protected Map<String, FieldManagerFactory> fieldsManagerFactories = new HashMap<>();
	
	private FieldManager fieldManager = null;
	
	private DataSource dataSource;
	private Connection connection = null;
	private ResultSet rs;
	private Boolean dataRead = null;
	private ObjectValue<XPOperand<?>> config;
	
	protected int _lineVisited = 0;
	
	public SQLDataReader(String name, DataSource dataSource/*, XPEvaluator evaluator, VariableContext variableContext*/, ObjectValue<XPOperand<?>> config, DMUtils dmu) throws DataException {
		super(name/*, evaluator, variableContext*/, dmu);
		this.dataSource = dataSource;
		this.config = config;
		
		fieldsManagerFactories.put("default", new FieldManagerFactory());
		fieldsManagerFactories.put("exa", new XAFieldManagerFactory());
	}
	
	@Override
	public boolean next() throws DataException {
		try {
			dataRead = rs.next();
			if(!dataRead) return false;
			++_lineVisited;
			return true;
			
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
			
			return rs.getString(fieldName);
		} catch (SQLException e) {
			throw new DataException(e);
		}
	}

	@Override
	public boolean open() throws DataException {
		
		try {
			//String drVariableName = DataManFactory.getDRVariableName(name);
			//evaluator.getCurrentVariableContext().addVariable(drVariableName, DataReader.class, this);
			
			from = config.getRequiredAttributAsString("from");
			
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
					Value<?, XPOperand<?>> vlField = mpFields.get(fname);
					
					String exp, type;
					BooleanValue<?> blField = vlField.asBooleanValue();
					if(blField == null) {
						StringValue<XPOperand<?>> sv = vlField.asStringValue();
						if(sv == null) {
							ObjectValue<XPOperand<?>> ov = vlField.asRequiredObjectValue();
							
							exp = ov.getAttributAsString("exp");
							type = ov.getAttributAsString("type", "string");
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
			
			/*for(DataReader<?> dr : dmu.getReaders().values()) {
				dr.open();
			}*/
			
			dmu.executeBeforeConnectionActions();
			
			connection = dataSource.getConnection();
			System.out.println("connexion open for Data reader" + this.hashCode());
			String sql = getSQL();
			
			System.out.println(sql);
			
			Statement stmt = connection.createStatement();
			rs = stmt.executeQuery(sql);
			
			
			return true;
		}
		catch (ManagedException|SQLException e) {
			throw new DataException(e);
		}
	}

	@Override
	public void close() throws DataException {
		/*for(DataReader<?> dr : dmu.getReaders().values()) {
			try { dr.close(); } catch(DataException e) { e.printStackTrace();}
		}*/
		
		dmu.clean();
		try {
			connection.close();
			System.out.println("connexion closed for Data reader" + this.hashCode());
		} catch (SQLException e) {
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
		SQLDataReader res = new SQLDataReader(name, dataSource/*, evaluator, variableContext*/, config, dmu);
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

}
