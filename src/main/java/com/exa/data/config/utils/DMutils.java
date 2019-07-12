package com.exa.data.config.utils;

import java.util.HashMap;
import java.util.Map;

import com.exa.data.DataReader;

import com.exa.data.config.DMFGeneral;
import com.exa.data.expression.macro.MCReaderStrValue;
import com.exa.data.expression.macro.Macro;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.parsing.XALParser;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;

public class DMutils {
	public static final String MC_READER_STR_VALUE = "reader-str-value";
	
	private Map<String, DataReader<?>> readers = new HashMap<>();
	
	private ObjectValue<XPOperand<?>> ovRoot;
	
	private DMFGeneral dmf;
	
	private Map<String, Macro<?>> macros = new HashMap<>();
	
	private VariableContext vc;
	
	private XALParser parser;
	
	private XPEvaluator evaluator;
	
	public void register(String name, DataReader<?> dr) {
		readers.put(name, dr);
	}
	
	public DMutils(DMFGeneral dmf, XALParser parser, ObjectValue<XPOperand<?>> ovRoot, XPEvaluator evaluator, VariableContext vc) {
		super();
		this.ovRoot = ovRoot;
		this.dmf = dmf;
		this.evaluator = evaluator;
		this.parser = parser;
		
		macros.put(MC_READER_STR_VALUE, new MCReaderStrValue(this));
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

	public DMFGeneral getDmf() { return dmf; }

	public VariableContext getVc() { return vc;	}

	public XPEvaluator getEvaluator() {	return evaluator; }

	public XALParser getParser() { return parser; }
	
	
}
