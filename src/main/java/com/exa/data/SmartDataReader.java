package com.exa.data;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.exa.data.MapReader.MapGetter;
import com.exa.data.action.Action;
import com.exa.data.config.DataManFactory;
import com.exa.data.config.DataManFactory.DMUSetup;
import com.exa.data.config.utils.DMUtils;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.lang.parsing.Computing;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public class SmartDataReader extends StandardDRWithDSBase<Field> {
	
	//private static final Logger LOG = LoggerFactory.getLogger(StandardDRWithDSBase.class);
	
	protected static final String FLW_MAIN = "main";
	
	protected static final String FLW_AFTER_MAIN = "after-main-next";
	
	protected static final String FLW_AFTER_MAIN_REOPEN = "after-main-next-reopen";

	protected Map<String, DataReader<?>> mainReaders = new LinkedHashMap<>();
	protected Map<String, DataMan> afterMainActions = new LinkedHashMap<>();
	protected Map<String, DataMan> afterMainActionsReopen = new LinkedHashMap<>();
	protected Map<String, DataMan> oneTimeActions = new LinkedHashMap<>();
	protected Map<String, DataMan> alwaysActions = new LinkedHashMap<>();
	protected Map<String, DataMan>  afterMainOneTimeActions = new LinkedHashMap<>();
	
	protected Map<String, SmartDataReader> subReaders = new LinkedHashMap<>();
	
	protected DataReader<?> currentMainReader = null;
	//protected boolean dataRead = false;

	private Iterator<DataReader<?>> drIndex = null;
	
	protected Integer _lineVisited = 0;
	
	private boolean dataInBuffer = false;
		
	
	public SmartDataReader(String name, ObjectValue<XPOperand<?>> config, FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMUtils dmu, DMUSetup dmuSetup, MapGetter mapGetter) {
		super(name, config, filesRepos, dataSources, defaultDataSource, dmu, dmuSetup);
		
		//DEBUG
		/*LOG.info(String.format("Smart Data reader : %s", name));
		for(String k : config.getValue().keySet()) {
			LOG.info(String.format("Inner Data reader : %s", k));
		}*/
	}
	
	public void addMainDataReader(String name, DataReader<?> dataReader) throws DataException {
		mainReaders.put(name, dataReader);
		
		dataReader.executeFieldsAction(field -> {
			String fieldName = field.getName();
			if(fields.containsKey(fieldName)) return;
			
			fields.put(fieldName, new Field(fieldName, field.getType()));
		});
	}

	@Override
	public boolean next() throws DataException {
		
		while(!currentMainReader.next()) {
			if(drIndex.hasNext()) {
				
				currentMainReader.close();
				currentMainReader = drIndex.next();
				
				//DEBUG
				//LOG.info(String.format("NEXT inner Smart Data reader"));
				
				currentMainReader.open();
				continue;
			}
			
			currentMainReader.close();
			return dataInBuffer = false;
		}
		
		dataInBuffer = true;
		++_lineVisited;
		
		for(DataMan dm : afterMainActions.values()) dm.execute();
		
		for(DataMan dm : afterMainActionsReopen.values()) {
			if(dm.isOpen()) try { dm.close(); } catch(DataException e) { e.printStackTrace(); }
			
			dm.open();
			
			DataReader<?> dr = dm.asDataReader();
			dr.executeFieldsAction(
				f -> {
					if(fields.containsKey(f.getName())) return;
					fields.put(f.getName(), f);
				}
			);
			dm.execute();
			//dm.close();
		}
		
		return true;
	}

	@Override
	public Field getField(String name) {
		return fields.get(name);
	}

	@Override
	public String getString(String fieldName) throws DataException {
		if(currentMainReader.containsField(fieldName)) return currentMainReader.getString(fieldName);
		
		for(DataMan dm : afterMainActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.lineVisited() == 0) continue;
			
			if(dr.containsField(fieldName))
				return dr.getString(fieldName);
		}
		
		for(DataMan dm : afterMainActionsReopen.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.lineVisited() == 0) continue;
			
			if(dr.containsField(fieldName))
				return dr.getString(fieldName);
		}
		return null;
	}

	@Override
	public boolean open() throws DataException {
		Map<String, Value<?, XPOperand<?>>> mpConfig = config.getValue();
		
		if(mainReaders.size() == 0)
		try {
			
			for(String drName :  mpConfig.keySet()) {
				if("type".equals(drName) || Computing.PRTY_PARAMS.equals(drName) || "fields".equals(drName)  || "beforeConnection".equals(drName) || "break".equals(drName)  || "onExecutionStarted".equals(drName) || drName.startsWith("_")) continue;
				
				
				ObjectValue<XPOperand<?>> ovDRConfig = config.getAttributAsObjectValue(drName);
				
				String type = ovDRConfig.getRequiredAttributAsString("type");
				
				DataManFactory dmf = dmFactories.get(type);
				if(dmf == null) throw new ManagedException(String.format("The DataReader type '%s' is unknown in SmartDataReader", type));
				
				DMUtils subDmu = dmu.newSubDmu(ovDRConfig.getAttributAsString("dataSource", dmf.getDefaultDataSource()));
				
				VariableContext vc = subDmu.getVc();
				
				updateVariableContext(ovDRConfig, dmu.getEvaluator(), vc, dmu.getVc());
				String flow  = ovDRConfig.getAttributAsString("flow");
				if(flow == null) flow = FLW_MAIN;
				
				DataReader<?> dr = getDataReader(ovDRConfig, drName, subDmu);
				
				vc.addVariable("this", DataReader.class, dr);
				vc.addVariable("parentDr", DataReader.class, this);
				
				if(FLW_MAIN.equals(flow)) {
					
					mainReaders.put(drName, dr);
					
					dr.executeFieldsAction(
						f -> {
							if(fields.containsKey(f.getName())) return;
							fields.put(f.getName(), f);
						}
					);
					continue;
				}
				
				if(FLW_AFTER_MAIN.equals(flow)) {
					afterMainActions.put(drName, dr);
					continue;
				}
				
				if(FLW_AFTER_MAIN_REOPEN.equals(flow)) {
					afterMainActionsReopen.put(drName, dr);
					
					
					continue;
				}
				
			}
			dmu.executeBeforeConnectionActions();
		} catch (ManagedException e) {
			if(e instanceof DataException) throw (DataException)e;
			throw new DataException(e);
		}
		
		drIndex = mainReaders.values().iterator();
		
		if(!drIndex.hasNext()) return dataInBuffer = false;
		currentMainReader = drIndex.next();
		currentMainReader.open();
		
		currentMainReader.executeFieldsAction(f -> {
			if(!fields.containsKey(f.getName())) fields.put(f.getName(), f);
		});
		
		for(DataMan dm : afterMainActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			
			dr.open();
			
			dr.executeFieldsAction(
				f -> {
					if(fields.containsKey(f.getName())) return;
					fields.put(f.getName(), f);
				}
			);
		}
		
		return dataInBuffer = true;
		
	}
	
	private DataReader<?> getDataReader(ObjectValue<XPOperand<?>> ovDRConfig, String drName, DMUtils subDmu/*, VariableContext vc*/) throws ManagedException {
		String type = ovDRConfig.getRequiredAttributAsString("type");
		
		DataManFactory dmf = dmFactories.get(type);
		
		if(dmf == null) throw new ManagedException(String.format("The DataReader type '%s' is unknown in SmartDataReader", type));
		
		ObjectValue<XPOperand<?>> ovBeforeConnectionActions = ovDRConfig.getAttributAsObjectValue("beforeConnection");
		if(ovBeforeConnectionActions != null) {
			Map<String, Value<?, XPOperand<?>>> mpBCA = ovBeforeConnectionActions.getValue();
			
			for(String bcaName: mpBCA.keySet()) {
				Action ac  = subDmu.registerBeforeConnectionAction(bcaName, mpBCA.get(bcaName));
				if(ac == null) throw new ManagedException(String.format("the action %s in 'beforeConnection' for entity '%s' seem to be invalid", bcaName, name));
			}
		}
		
		ObjectValue<XPOperand<?>> ovOnExecutionStarted = ovDRConfig.getAttributAsObjectValue("onExecutionStarted");
		if(ovOnExecutionStarted != null) {
			Map<String, Value<?, XPOperand<?>>> mpBCA = ovOnExecutionStarted.getValue();
			
			for(String bcaName: mpBCA.keySet()) {
				Action ac = subDmu.registerOnExecutionStartedAction(bcaName, mpBCA.get(bcaName));
				if(ac == null) throw new ManagedException(String.format("the action %s in 'onExecutionStarted' for entity '%s' seem to be invalid", bcaName, name));
			}
		}
		
		
		DataReader<?> res = dmf.getDataReader(drName, ovDRConfig/*, evaluator, vc*/, subDmu);
		
		//res.setEvaluator(evaluator);
		return res;
	}

	@Override
	public void close() throws DataException {
		drIndex = null;
		_lineVisited = 0;
		
		dmu.clean();
		
		for(DataMan dm : afterMainActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			try { dr.close(); } catch(DataException e) { e.printStackTrace();}
		}
		
		for(DataMan dm : afterMainActionsReopen.values()) {
			DataReader<?> dr = dm.asDataReader();
			
			if(dr == null) continue;
			
			try { dr.close(); } catch(DataException e) { e.printStackTrace(); }
		}
		
		for(DataMan dm : oneTimeActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			try { dr.close(); } catch(DataException e) { e.printStackTrace();}
		}
		
		for(DataMan dm : alwaysActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			dr.close();
		}
		
		for(DataMan dm : afterMainOneTimeActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			try { dr.close(); } catch(DataException e) { e.printStackTrace();}
		}
		
		for(DataReader<?> dr : mainReaders.values()) {
			try { dr.close(); } catch(DataException e) { e.printStackTrace();}
		}
		
		
		
	}

	@Override
	public boolean isOpen() {
		return drIndex != null;
	}

	@Override
	public Date getDate(String fieldName) throws DataException {
		if(currentMainReader.containsField(fieldName)) return currentMainReader.getDate(fieldName);
		
		for(DataMan dm : afterMainActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.lineVisited() == 0) continue;
			
			if(dr.containsField(fieldName))
				return dr.getDate(fieldName);
		}
		
		for(DataMan dm : afterMainActionsReopen.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.lineVisited() == 0) continue;
			
			if(dr.containsField(fieldName))
				return dr.getDate(fieldName);
		}
		return null;
	}

	@Override
	public Double getDouble(String fieldName) throws DataException {
		if(currentMainReader.containsField(fieldName)) return currentMainReader.getDouble(fieldName);
		for(DataMan dm : afterMainActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.lineVisited() == 0) continue;
			
			if(dr.containsField(fieldName)) 
				return dr.getDouble(fieldName);
			
		}
		
		for(DataMan dm : afterMainActionsReopen.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.lineVisited() == 0) continue;
			
			if(dr.containsField(fieldName)) 
				return dr.getDouble(fieldName);
			
		}
		return null;
	}

	@Override
	public SmartDataReader cloneDM() {
		return new SmartDataReader(name, config, filesRepos, dataSources, defaultDataSource, dmu, dmuSetup, /* TO FIX*/ () -> new HashMap<>());
	}

	@Override
	public Object getObject(String fieldName) throws DataException {
		if(currentMainReader.containsField(fieldName)) return currentMainReader.getObject(fieldName);
		for(DataMan dm : afterMainActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.lineVisited() == 0) continue;
			
			if(dr.containsField(fieldName))
				return dr.getObject(fieldName);
		}
		
		for(DataMan dm : afterMainActionsReopen.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.lineVisited() == 0) continue;
			
			if(dr.containsField(fieldName))
				return dr.getObject(fieldName);
		}
		return null;
	}

	@Override
	public int lineVisited() { return _lineVisited; }

	@Override
	public Integer getInteger(String fieldName) throws DataException {
		if(currentMainReader.containsField(fieldName)) return currentMainReader.getInteger(fieldName);
		for(DataMan dm : afterMainActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.lineVisited() == 0) continue;
			
			if(dr.containsField(fieldName))
				return dr.getInteger(fieldName);
		}
		
		for(DataMan dm : afterMainActionsReopen.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.lineVisited() == 0) continue;
			
			if(dr.containsField(fieldName))
				return dr.getInteger(fieldName);
		}
		return null;
	}

	@Override
	public boolean dataInBuffer() { return dataInBuffer; }
	
}
