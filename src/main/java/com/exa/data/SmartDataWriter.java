package com.exa.data;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.exa.data.MapReader.MapGetter;
import com.exa.data.action.Action;
import com.exa.data.config.DataManFactory;
import com.exa.data.config.DataManFactory.DMUSetup;
import com.exa.data.config.utils.BreakProperty;
import com.exa.data.config.utils.DMUtils;
import com.exa.data.config.utils.DataUserException;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.expression.XALCalculabeValue;
import com.exa.lang.parsing.Computing;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public class SmartDataWriter extends StandardDWWithDSBase<Field> {
	
	protected static final String FLW_MAIN = "main";
	
	protected static final String FLW_AFTER_MAIN = "after-main-next";
	
	protected Map<String, DataWriter<?>> mainDataWriters = new LinkedHashMap<>();
	
	protected Map<String, DataWriter<?>> afterMainActions = new LinkedHashMap<>();
	
	protected DataWriter<?> currentMainDataWriter = null;
	
	private boolean opened = false;
	
	private List<BreakProperty> breakProperties = new ArrayList<>();
	
	public SmartDataWriter(String name, ObjectValue<XPOperand<?>> config, FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DataReader<?> drSource, DMUtils dmu, DMUSetup dmuSetup, MapGetter mapGetter) {
		super(name, config, filesRepos, dataSources, defaultDataSource, drSource, dmu, dmuSetup);
	}
	
	public void addMainDataWriter(String name, DataWriter<?> dataWriter) throws DataException {
		mainDataWriters.put(name, dataWriter);
	}

	@Override
	public int update(DataReader<?> dr) throws DataException {
		try {
			if(mustBreak()) return 0;
			for(DataWriter<?> dw :  mainDataWriters.values()) {	dw.open();	}
			
			boolean mainOK = false;
			
			for(DataWriter<?> dw :  mainDataWriters.values()) {
				mainOK = mainOK || dw.execute();
			}
			
			if(mainOK) {
				try {
					for(DataWriter<?> dw :  afterMainActions.values()) { dw.open();	}
					
					for(DataWriter<?> dw :  afterMainActions.values()) {
						dw.execute();
					}
					
					for(DataWriter<?> dw :  afterMainActions.values()) { dw.close(); }
				}
				catch(ManagedException e) {
					if(e instanceof DataException) throw (DataException)e;
					throw new DataException(e);
				}
				finally {
					for(DataWriter<?> dw :  afterMainActions.values()) {	try { dw.close(); } catch(Exception e) { e.printStackTrace(); }	}
				}
			}
			
			for(DataWriter<?> dw :  mainDataWriters.values()) {	dw.close();	}
		}
		catch(ManagedException e) {
			if(e instanceof DataException) throw (DataException)e;
			throw new DataException(e);
		}
		finally {
			for(DataWriter<?> dw :  mainDataWriters.values()) {	try { dw.close(); } catch(Exception e) { e.printStackTrace(); }	}
		}
		
		return 0;
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
	public boolean open() throws DataException {
		
		Map<String, Value<?, XPOperand<?>>> mpConfig = config.getValue();
		
		try {
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
			
			for(String drName :  mpConfig.keySet()) {
				if("type".equals(drName) || Computing.PRTY_PARAMS.equals(drName) || "fields".equals(drName)  || "beforeConnection".equals(drName) || "break".equals(drName)  || "onExecutionStarted".equals(drName) || drName.startsWith("_")) continue;
				
				ObjectValue<XPOperand<?>> ovDRConfig = config.getAttributAsObjectValue(drName);
				
				
				String type = ovDRConfig.getRequiredAttributAsString("type");
				
				DataManFactory dmf = dmFactories.get(type);
				if(dmf == null) throw new ManagedException(String.format("The DataReader type '%s' is unknown in SmartDataReader", type));
				
				
				DMUtils subDmu = dmu.newSubDmu(ovDRConfig.getAttributAsString("dataSource", dmf.getDefaultDataSource()));
				VariableContext vc = subDmu.getVc();
				vc.addVariable("dmu", DMUtils.class, subDmu);
				
				ObjectValue<XPOperand<?>> ovParams = ovDRConfig.getAttributAsObjectValue(Computing.PRTY_PARAMS);
				if(ovParams != null) {
					Map<String, Value<?, XPOperand<?>>> mpParams = ovParams.getValue();
					
					for(String param : mpParams.keySet()) {
						vc.addVariable(param, dmu.getEvaluator().getClassesMan().getType(mpParams.get(param).asString()).valueClass(), null);
					}
				}
				
				updateVariableContext(ovDRConfig, dmu.getEvaluator(), vc, dmu.getVc());
				String flow  = ovDRConfig.getAttributAsString("flow");
				if(flow == null) flow = FLW_MAIN;
				
				DataWriter<?> dr = getDataWriter(ovDRConfig, drName, subDmu);
				
				vc.addVariable("this", DataWriter.class, dr);
				
				if(FLW_MAIN.equals(flow)) {
					
					mainDataWriters.put(drName, dr);
					continue;
				}
				
				if(FLW_AFTER_MAIN.equals(flow)) {
					afterMainActions.put(drName, dr);
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
		breakProperties.clear();
		
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
	
	public static void updateVariableContext(ObjectValue<XPOperand<?>> ov, XPEvaluator evaluator, VariableContext vc, VariableContext parentVC) {
		Map<String, Value<?, XPOperand<?>>> mp = ov.getValue();
		
		for(String propertyName : mp.keySet()) {
			Value<?, XPOperand<?>> vl=mp.get(propertyName);
			
			ObjectValue<XPOperand<?>> vov = vl.asObjectValue();
			if(vov != null) {
				updateVariableContext(vov, evaluator, vc, parentVC);
				continue;
			}
			
			ArrayValue<XPOperand<?>> av = vl.asArrayValue();
			if(av != null) {
				List<Value<?, XPOperand<?>>> lst = av.getValue();
				
				for(Value<?, XPOperand<?>> vlItem : lst) {
					updateVariableContext(vlItem, evaluator, vc, parentVC);
				}
			}
			
			CalculableValue<?, XPOperand<?>> cl = vl.asCalculableValue();
			if(cl == null) continue;
			
			XALCalculabeValue<?> xalCL = (XALCalculabeValue<?>) cl;
			
			VariableContext clVc = xalCL.getVariableContext();
			
			if(clVc == parentVC) xalCL.setVariableContext(vc);
			
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
	
	private static void updateVariableContext(Value<?, XPOperand<?>> vl, XPEvaluator evaluator, VariableContext vc, VariableContext parentVC) {
		ObjectValue<XPOperand<?>> vov = vl.asObjectValue();
		if(vov != null) {
			updateVariableContext(vov, evaluator, vc, parentVC);
			return;
		}
		
		ArrayValue<XPOperand<?>> av = vl.asArrayValue();
		if(av != null) {
			List<Value<?, XPOperand<?>>> lst = av.getValue();
			
			for(Value<?, XPOperand<?>> vlItem : lst) {
				updateVariableContext(vlItem, evaluator, vc, parentVC);
			}
		}
		
		CalculableValue<?, XPOperand<?>> cl = vl.asCalculableValue();
		if(cl == null) return;
		
		XALCalculabeValue<?> xalCL = (XALCalculabeValue<?>) cl;
		
		VariableContext clVc = xalCL.getVariableContext();
		
		if(clVc == parentVC) xalCL.setVariableContext(vc);
		/*else if(clVc != null) {
			if(clVc.getParent() == parentVC) clVc.setParent(vc);
		}*/
		
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
				if(ac == null) throw new ManagedException(String.format("the action %s in 'onExecutionStarted' for entity '%s' seem to be invalid", bcaName, name));
			}
		}
		
		DataWriter<?> res = dmf.getDataWriter(dwName, ovDRConfig, drSource, subDmu, false, false);
		
		return res;
	}

}
