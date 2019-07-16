package com.exa.data.config.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.action.ASAssignment;
import com.exa.data.action.Action;
import com.exa.data.action.ActionSeeker;
import com.exa.data.config.DMFGeneral;
import com.exa.data.config.DataManFactory;
import com.exa.data.config.DataManFactory.DMUSetup;
import com.exa.data.expression.macro.MCReaderStrValue;
import com.exa.data.expression.macro.Macro;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.MapVariableContext;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.parsing.Computing;

import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public class DMUtils {
	public static final String MC_READER_STR_VALUE = "reader-str-value";
	
	private Map<String, DataReader<?>> readers = new HashMap<>();
	
	protected Map<String, ?> params = new HashMap<>();
	
	private List<DataReader<?>> drToClose = new ArrayList<>();
	
	private List<Action> beforeConnectionActions = new ArrayList<>();
	
	private List<ActionSeeker> actionSeekers = new ArrayList<>();
	
	private ObjectValue<XPOperand<?>> ovRoot;
	
	private DMFGeneral dmf;
	
	private Map<String, Macro<?>> macros = new HashMap<>();
	
	private VariableContext vc;
	
	//private XALParser parser;
	
	private XPEvaluator evaluator;
	
	private DMUSetup dmuSetup;
	
	public DMUtils(DMFGeneral dmf/*, XALParser parser*/, ObjectValue<XPOperand<?>> ovRoot, XPEvaluator evaluator, VariableContext vc, DMUSetup dmuSetup) {
		super();
		this.ovRoot = ovRoot;
		this.dmf = dmf;
		this.evaluator = evaluator;
		this.vc = vc;
		//this.parser = parser;
		this.dmuSetup = dmuSetup;
		
		macros.put(MC_READER_STR_VALUE, new MCReaderStrValue(this));
		
		actionSeekers.add(new ASAssignment(this));
	}
	
	public void register(String name, DataReader<?> dr) {
		readers.put(name, dr);
	}
	
	public void register(String type, Macro<?> macro) {
		macros.put(type, macro);
	}
	
	public void registerBeforeAction(Action action) {
		beforeConnectionActions.add(action);
	}
	
	public Action registerBeforeAction(String name, Value<?, XPOperand<?>> value) {
		for(ActionSeeker as : actionSeekers) {
			Action res = as.found(name, value);
			
			if(res == null) continue;
			
			beforeConnectionActions.add(res);
			
			return res;
		}
		
		return null;
	}

	public ObjectValue<XPOperand<?>> getOvRoot() {
		return ovRoot;
	}

	public DataReader<?> getReader(String name) { return readers.get(name); }

	public Map<String, DataReader<?>> getReaders() { return readers; }
	
	public String evalString(String macroRef) throws ManagedException {
		ObjectValue<XPOperand<?>> ovMacro = ovRoot.getPathAttributAsObjecValue(macroRef);
		
		if(ovMacro == null) throw new ManagedException(String.format("'%s' macro is not defined", macroRef));
		
		String macroType = ovMacro.getRequiredAttributAsString("type");
		
		Macro<?> macro = macros.get(macroType);
		if(macro == null) throw new ManagedException(String.format("macro type '%s' is not defined", macroType));
		
		Macro<String> typMacro = macro.asMacroString();
		
		return typMacro.value(macroRef, ovMacro);
	}
	
	
	public DataReader<?> evalDataReader(String macroRef) throws ManagedException {
		ObjectValue<XPOperand<?>> ovMacro = ovRoot.getPathAttributAsObjecValue(macroRef);
		
		if(ovMacro == null) throw new ManagedException(String.format("'%s' macro is not defined", macroRef));
		
		String macroType = ovMacro.getRequiredAttributAsString("type");
		
		Macro<?> macro = macros.get(macroType);
		if(macro == null) throw new ManagedException(String.format("macro type '%s' is not defined", macroType));
		
		Macro<DataReader<?>> typMacro = macro.asMacroDataReader();
		
		return typMacro.value(macroRef, ovMacro);
	}

	public Map<String, ?> getParams() {
		return params;
	}

	public void setParams(Map<String, ?> params) {
		this.params = params;
	}

	public DMFGeneral getDmf() { return dmf; }

	public VariableContext getVc() { return vc;	}

	public XPEvaluator getEvaluator() {	return evaluator; }

	//public XALParser getParser() { return parser; }
	
	
	public DMUtils newSubDmu(VariableContext vc) { return new DMUtils(dmf/*, parser*/, ovRoot, evaluator, vc, dmuSetup); }
	
	
	public DataReader<?> loadReader(String readerRef) throws ManagedException {
		ObjectValue<XPOperand<?>> ovEntity = ovRoot.getPathAttributAsObjecValue(String.format("entities.%s", readerRef));
		if(ovEntity == null) throw new ManagedException(String.format("The path '%s' could be found", readerRef));
		ObjectValue<XPOperand<?>> ovReader = Computing.object(DataManFactory.parser, ovEntity, evaluator, vc, Computing.getDefaultObjectLib(ovRoot));
		 
		DMUtils dmu = newSubDmu(new MapVariableContext(vc));
		dmuSetup.setup(dmu);
		
		DataReader<?> res = dmf.getDataReader(readerRef, ovReader, dmu);
		
		res.open(); res.next();
		
		drToClose.add(res);
		
		return res;
	}
	
	public DataReader<?> openReader(String readerRef) throws ManagedException {
		ObjectValue<XPOperand<?>> ovEntity = ovRoot.getPathAttributAsObjecValue(String.format("entities.%s", readerRef));
		if(ovEntity == null) throw new ManagedException(String.format("The path '%s' could be found", readerRef));
		ObjectValue<XPOperand<?>> ovReader = Computing.object(DataManFactory.parser, ovEntity, evaluator, vc, Computing.getDefaultObjectLib(ovRoot));
		 
		DMUtils dmu = newSubDmu(new MapVariableContext(vc));
		DataReader<?> res = dmf.getDataReader(readerRef, ovReader, dmu);
		
		res.open();
		
		drToClose.add(res);
		
		return res;
	}

	public void executeBeforeConnectionActions() throws ManagedException {
		for(Action action : beforeConnectionActions) {
			action.execute();
		}
	}
	
	public void clean() {
		for(DataReader<?> dr : drToClose) {
			try {dr.close();} catch(Exception e) {}
		}
		drToClose.clear();
	}

	
	
}
