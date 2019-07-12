package com.exa.data;

import java.util.Date;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import com.exa.data.MapReader.MapGetter;
import com.exa.data.config.DataManFactory;
import com.exa.data.config.utils.DMutils;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.MapVariableContext;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public class SmartDataReader extends StandardDRWithDSBase<Field> {
	
	protected static final String FLW_MAIN = "main";
	
	protected static final String FLW_AFTER_MAIN = "after-main-next";

	protected Map<String, DataReader<?>> mainReaders = new LinkedHashMap<>();
	protected Map<String, DataMan> afterMainActions = new LinkedHashMap<>();
	protected Map<String, DataMan> oneTimeActions = new LinkedHashMap<>();
	protected Map<String, DataMan> alwaysActions = new LinkedHashMap<>();
	protected Map<String, DataMan>  afterMainOneTimeActions = new LinkedHashMap<>();
	
	protected Map<String, SmartDataReader> subReaders = new LinkedHashMap<>();
	
	protected DataReader<?> currentMainReader = null;
	protected boolean dataRead = false;

	private Iterator<DataReader<?>> drIndex = null;
	
	protected Integer _lineVisited = 0;
		
	
	public SmartDataReader(String name, ObjectValue<XPOperand<?>> config/*, XPEvaluator evaluator, VariableContext variableContext*/, FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMutils dmu, MapGetter mapGetter) {
		super(name, config/*, evaluator, variableContext*/, filesRepos, dataSources, defaultDataSource, dmu);
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
				currentMainReader.open();
			}
			else currentMainReader.close();
			return dataRead = false;
		}
		
		dataRead = true;
		++_lineVisited;
		
		for(DataMan dm : afterMainActions.values()) dm.execute();
		
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
			
			if(dr.containsField(fieldName))
				return dr.getString(fieldName);
		}
		return null;
	}

	@Override
	public boolean open() throws DataException {
		Map<String, Value<?, XPOperand<?>>> mpConfig = config.getValue();
		
		try {
			
			XPEvaluator evaluator = dmu.getEvaluator();
			
			for(String drName :  mpConfig.keySet()) {
				if("type".equals(drName) || drName.startsWith("_")) continue;
				
				VariableContext vc = new MapVariableContext(dmu.getVc()/*evaluator.getCurrentVariableContext()*/);
				
				ObjectValue<XPOperand<?>> ovDRConfig = config.getAttributAsObjectValue(drName); //Computing.object(config, drName, evaluator, vc);
				updateVariableContext(ovDRConfig, vc, dmu.getVc() /*evaluator.getCurrentVariableContext()*/);
				String flow  = ovDRConfig.getAttributAsString("flow");
				if(flow == null) flow = FLW_MAIN;
				
				DMutils subDmu = dmu.newSubDmu(vc);
				
				DataReader<?> dr = getDataReader(ovDRConfig, drName, subDmu);
				
				vc.addVariable("this", DataReader.class, dr);
				
				if(FLW_MAIN.equals(flow)) {
					
					mainReaders.put(drName, dr);
					continue;
				}
				
				if(FLW_AFTER_MAIN.equals(flow)) {
					afterMainActions.put(drName, dr);
					continue;
				}
				
			}
		} catch (ManagedException e) {
			throw new DataException(e);
		}
		
		drIndex = mainReaders.values().iterator();
		
		if(!drIndex.hasNext()) return dataRead = false;
		currentMainReader = drIndex.next();
		currentMainReader.open();
		
		for(DataReader<?> dr : dmu.getReaders().values()) {
			dr.open();
		}
		
		for(DataMan dm : afterMainActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			dr.open();
		}
		
		return dataRead = true;
		
	}
	
	private DataReader<?> getDataReader(ObjectValue<XPOperand<?>> ovDRConfig, String drName, DMutils subDmu/*, VariableContext vc*/) throws ManagedException {
		String type = ovDRConfig.getRequiredAttributAsString("type");
		
		DataManFactory dmf = dmFactories.get(type);
		
		if(dmf == null) throw new ManagedException(String.format("the type %s is unknown", type));
		
		
		DataReader<?> res = dmf.getDataReader(drName, ovDRConfig/*, evaluator, vc*/, subDmu);
		
		//res.setEvaluator(evaluator);
		return res;
	}

	@Override
	public void close() throws DataException {
		for(DataReader<?> dr : dmu.getReaders().values()) {
			try { dr.close(); } catch(DataException e) { e.printStackTrace();}
		}
		
		for(DataMan dm : afterMainActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			try { dr.close(); } catch(DataException e) { e.printStackTrace();}
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
			
			if(dr.containsField(fieldName)) 
				return dr.getDouble(fieldName);
			
		}
		return null;
	}

	@Override
	public SmartDataReader cloneDM() {
		
		return null;
	}

	@Override
	public Object getObject(String fieldName) throws DataException {
		if(currentMainReader.containsField(fieldName)) return currentMainReader.getObject(fieldName);
		for(DataMan dm : afterMainActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.containsField(fieldName))
				return dr.getObject(fieldName);
		}
		return null;
	}

	@Override
	public int lineVisited() {
		return _lineVisited;
	}

	@Override
	public Integer getInteger(String fieldName) throws DataException {
		if(currentMainReader.containsField(fieldName)) return currentMainReader.getInteger(fieldName);
		for(DataMan dm : afterMainActions.values()) {
			DataReader<?> dr = dm.asDataReader();
			if(dr == null) continue;
			
			if(dr.containsField(fieldName))
				return dr.getInteger(fieldName);
		}
		return null;
	}
	
}
