package com.exa.data;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.exa.data.config.DMFLibre;
import com.exa.data.config.DMFMap;
import com.exa.data.config.DMFRowToField;
import com.exa.data.config.DMFSmart;
import com.exa.data.config.DMFSpSql;
import com.exa.data.config.DMFSql;
import com.exa.data.config.DMFWebService;
import com.exa.data.config.DMFXLiteral;
import com.exa.data.config.DataManFactory;
import com.exa.data.config.DataManFactory.DMUSetup;
import com.exa.data.config.utils.DMUtils;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.expression.XALCalculabeValue;
import com.exa.lang.parsing.Computing;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public abstract class StandardDataReaderBase<_FIELD extends Field> implements DataReader<_FIELD> {
	protected Map<String, _FIELD> fields = new LinkedHashMap<>();
	
	protected Map<String, DataMan> subDataManagers = new LinkedHashMap<>();
	
	protected DataReader<?> parent;
	
	protected String name;
	
	protected DMUtils dmu;
	
	protected boolean shouldCloseConnection = true;
	
	protected Map<String, DataManFactory> dmFactories = new HashMap<>();
	
	//protected Value<?, XPOperand<?>> openCondition = new BooleanValue<>(Boolean.TRUE);
	
	public StandardDataReaderBase(String name, DMUtils dmu, boolean initFactories, FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMUSetup dmuSetup) {
		super();
		this.name = name;
		
		this.dmu = dmu;
		
		if(initFactories) initializeDataFactories(filesRepos, dataSources, defaultDataSource, dmuSetup);
		
		
	}
	
	public StandardDataReaderBase(String name, DMUtils dmu) {
		this(name, dmu, false, null, null, null, null);
	}
	
	public void initializeDataFactories( FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMUSetup dmuSetup) {
		dmFactories.put(DataManFactory.DMFN_LIBRE, new DMFLibre(filesRepos, dataSources, defaultDataSource, dmuSetup));
		
		dmFactories.put(DataManFactory.DMFN_XLITERAL, new DMFXLiteral(filesRepos, dataSources, defaultDataSource, dmuSetup));
		
		dmFactories.put(DataManFactory.DMFN_SMART, new DMFSmart(filesRepos, dataSources, defaultDataSource, dmuSetup));
		
		dmFactories.put(DataManFactory.DMFN_ROW_TO_FIELD, new DMFRowToField(filesRepos, dataSources, defaultDataSource, dmuSetup));
		
		dmFactories.put(DataManFactory.DMFN_WS, new DMFWebService(filesRepos, dataSources, defaultDataSource, dmuSetup));
		
		dmFactories.put(DataManFactory.DMFN_MAP, new DMFMap(filesRepos, dataSources, defaultDataSource, dmuSetup));
		
		dmFactories.put(DataManFactory.DMFN_SP_SQL, new DMFSpSql(filesRepos, dataSources, defaultDataSource, dmuSetup));
		
		DataManFactory dmf = new DMFSql(filesRepos, dataSources, defaultDataSource, dmuSetup);
		
		dmFactories.put(DataManFactory.DMFN_SQL, dmf);
		dmFactories.put("tsql", dmf);
		dmFactories.put("t-sql", dmf);
		dmFactories.put("transact-sql", dmf);
		dmFactories.put("plsql", dmf);
		dmFactories.put("pl-sql", dmf);
		dmFactories.put("sql-server", dmf);
		dmFactories.put("sql-oracle", dmf);
		dmFactories.put("oracle", dmf);
		
		for(DataManFactory dmFactory : dmFactories.values()) { dmFactory.initialize(); }
	}
	
	@Override
	public boolean execute() throws DataException {
		try {
			dmu.executeOnExecutionStarted();
		} catch (ManagedException e) {
			if(e instanceof DataException) throw (DataException)e;
			throw new DataException(e);
		}
		return next();
	}

	@Override
	public void executeFieldsAction(FieldAction<_FIELD> fa) throws DataException {
		
		for(_FIELD field : fields.values()) {
			fa.execute(field);
		}
	}

	@Override
	public _FIELD getField(String name) {
		return fields.get(name);
	}

	@Override
	public boolean containsField(String fieldName) {
		return fields.containsKey(fieldName);
	}
	
	@Override
	public abstract StandardDataReaderBase<_FIELD> cloneDM() throws DataException;
	
	@Override
	public DataMan getSubDataMan(String name) {
		return subDataManagers.get(name);
	}

	@Override
	public Object getObject(String fieldName) throws DataException {
		_FIELD field = fields.get(fieldName);
		if(field == null) return null; //throw new DataException(String.format("Unknown field name %s in data reader %s", fieldName, name));
		
		if("int".equals(field.getType()) || "integer".equals(field.getType())) return getInteger(fieldName);
		
		if("date".equals(field.getType()) || "datetime".equals(field.getType())  || "time".equals(field.getType())) return getDate(fieldName);
		
		if("double".equals(field.getType()) || "float".equals(field.getType()) || "decimal".equals(field.getType())) return getDouble(fieldName);
		
		return getString(fieldName);
	}

	@Override
	public XPEvaluator getEvaluator() {
		return dmu.getEvaluator();
	}

	@Override
	public DataReader<?> getParent() {
		return parent;
	}

	@Override
	public void setParent(DataReader<?> parent) {
		this.parent = parent;
	}

	@Override
	public StandardDataReaderBase<_FIELD> asDataReader() {
		return this;
	}
	
	@Override
	public DataWriter<?> asDataWriter() {
		return null;
	}

	
	public static void updateVariableContext(ObjectValue<XPOperand<?>> ov, XPEvaluator evaluator, VariableContext vc, VariableContext parentVC) {
		Map<String, Value<?, XPOperand<?>>> mp = ov.getValue();
		
		for(String propertyName : mp.keySet()) {
			Value<?, XPOperand<?>> vl=mp.get(propertyName);
			
			
			@SuppressWarnings("rawtypes")
			CalculableValue cl = vl.asCalculableValue();
			
			if(cl != null) {
				XALCalculabeValue<?> xalCL = (XALCalculabeValue<?>) cl;
				if(xalCL.getVariableContext() == parentVC) xalCL.setVariableContext(vc);
				//continue;
			}
			else {
				ObjectValue<XPOperand<?>> vov = vl.asObjectValue();
				if(vov != null) {
					updateVariableContext(vov, evaluator, vc, parentVC);
					continue;
				}
			}
			

			/*CalculableValue<?, XPOperand<?>> cl = vl.asCalculableValue();
			if(cl == null) continue;
			
			XALCalculabeValue<?> xalCL = (XALCalculabeValue<?>) cl;
			if(xalCL.getVariableContext() == parentVC) xalCL.setVariableContext(vc);*/
			
			Set<VariableContext> vcs = evaluator.getRegisteredVariableContexts(Computing.VCC_CALLS);
			
			if(vcs != null) {
			
				Set<VariableContext> vcsToRemove = new HashSet<>();
				
				for(VariableContext ivc : vcs) {
					if(ivc.getParent() == vc) { vcsToRemove.add(ivc); continue;}
					if(ivc.getParent() == parentVC) {
						ivc.setParent(vc);
						vcsToRemove.add(ivc); 
					}
				}
				
				for(VariableContext ivc : vcsToRemove) {
					evaluator.unregisterVariableContext(Computing.VCC_CALLS, ivc);
				}
			}
			
		}
	}


	
	
}
