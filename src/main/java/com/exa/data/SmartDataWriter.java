package com.exa.data;

import java.util.LinkedHashMap;
import java.util.Map;

import com.exa.data.MapReader.MapGetter;
import com.exa.data.action.Action;
import com.exa.data.config.DataManFactory;
import com.exa.data.config.DataManFactory.DMUSetup;
import com.exa.data.config.utils.DMUtils;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.MapVariableContext;
import com.exa.lang.expression.XALCalculabeValue;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public class SmartDataWriter extends StandardDWWithDSBase<Field> {
	
	protected static final String FLW_MAIN = "main";
	
	protected static final String FLW_AFTER_MAIN = "after-main-next";
	
	protected Map<String, DataWriter<?>> mainDataWriters = new LinkedHashMap<>();
	
	protected DataWriter<?> currentMainDataWriter = null;
	
	private boolean opened = false;
	
	public SmartDataWriter(String name, ObjectValue<XPOperand<?>> config, FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DataReader<?> drSource, DMUtils dmu, DMUSetup dmuSetup, MapGetter mapGetter) {
		super(name, config, filesRepos, dataSources, defaultDataSource, drSource, dmu, dmuSetup);
	}
	
	public void addMainDataWriter(String name, DataWriter<?> dataWriter) throws DataException {
		mainDataWriters.put(name, dataWriter);
	}

	@Override
	public int update(DataReader<?> dr) throws DataException {
		try {
			for(DataWriter<?> dw :  mainDataWriters.values()) {	dw.open();	}
			
			for(DataWriter<?> dw :  mainDataWriters.values()) {
				dw.execute();
			}
			
			for(DataWriter<?> dw :  mainDataWriters.values()) {	dw.close();	}
		}
		finally {
			for(DataWriter<?> dw :  mainDataWriters.values()) {	try { dw.close(); } catch(Exception e) { e.printStackTrace(); }	}
		}
		
		return 0;
	}

	@Override
	public boolean open() throws DataException {
		
		Map<String, Value<?, XPOperand<?>>> mpConfig = config.getValue();
		
		try {
			for(String drName :  mpConfig.keySet()) {
				if("type".equals(drName) || "fields".equals(drName) || drName.startsWith("_")) continue;
				
				VariableContext vc = new MapVariableContext(dmu.getVc());
				ObjectValue<XPOperand<?>> ovDRConfig = config.getAttributAsObjectValue(drName);
				
				updateVariableContext(ovDRConfig, vc, dmu.getVc());
				String flow  = ovDRConfig.getAttributAsString("flow");
				if(flow == null) flow = FLW_MAIN;
				
				DMUtils subDmu = dmu.newSubDmu(vc);
				
				DataWriter<?> dr = getDataWriter(ovDRConfig, drName, subDmu);
				
				vc.addVariable("this", DataWriter.class, dr);
				
				if(FLW_MAIN.equals(flow)) {
					
					mainDataWriters.put(drName, dr);
					continue;
				}
			}
			
			dmu.executeBeforeConnectionActions();
			
			opened = true;
			
		} catch (ManagedException e) {
			if(e instanceof DataException) throw (DataException)e;
			throw new DataException(e);
		}
		
		return false;
	}

	@Override
	public void close() throws DataException {
		dmu.clean();
		
		for(DataWriter<?> dm : mainDataWriters.values()) {
			try { dm.close(); } catch(DataException e) { e.printStackTrace();}
		}
		
		mainDataWriters.clear();
		opened = false;
		
	}

	@Override
	public DataWriter<Field> cloneDM() throws DataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isOpen() {
		return opened;
	}
	
	public static void updateVariableContext(ObjectValue<XPOperand<?>> ov, VariableContext vc, VariableContext prentVC) {
		Map<String, Value<?, XPOperand<?>>> mp = ov.getValue();
		
		for(String propertyName : mp.keySet()) {
			Value<?, XPOperand<?>> vl=mp.get(propertyName);
			
			ObjectValue<XPOperand<?>> vov = vl.asObjectValue();
			if(vov != null) {
				updateVariableContext(vov, vc, prentVC);
				continue;
			}
			
			CalculableValue<?, XPOperand<?>> cl = vl.asCalculableValue();
			if(cl == null) continue;
			
			XALCalculabeValue<?> xalCL = (XALCalculabeValue<?>) cl;
			if(xalCL.getVariableContext() == prentVC) xalCL.setVariableContext(vc);
			
		}
	}
	
	private DataWriter<?> getDataWriter(ObjectValue<XPOperand<?>> ovDRConfig, String dwName, DMUtils subDmu) throws ManagedException {
		String type = ovDRConfig.getRequiredAttributAsString("type");
		
		DataManFactory dmf = dmFactories.get(type);
		
		if(dmf == null) throw new ManagedException(String.format("the type '%s' is unknown is a smart data writer", type));
		
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
				if(ac == null) throw new ManagedException(String.format("the action %s in 'beforeExecution' for entity '%s' seem to be invalid", bcaName, name));
			}
		}
		
		DataWriter<?> res = dmf.getDataWriter(dwName, ovDRConfig, drSource, subDmu, false, false);
		
		return res;
	}

}
