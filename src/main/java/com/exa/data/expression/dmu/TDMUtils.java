package com.exa.data.expression.dmu;

import com.exa.data.DataReader;
import com.exa.data.config.utils.DMUtils;

import com.exa.eva.OperatorManager.OMOperandType;
import com.exa.expression.OMMethod;
import com.exa.expression.Type;

import com.exa.expression.types.TObjectClass;

public class TDMUtils extends TObjectClass<DMUtils, Object> {

	public TDMUtils() {
		super(null, DMUtils.class, "DMUtils");
	}
	
	@Override
	public void initialize() {
		@SuppressWarnings("rawtypes")
		OMMethod<DataReader> omReader = new OMMethod<>("reader", 2, OMOperandType.POST_OPERAND);
		omReader.addOperator(new MtdReader());
		methods.put("reader", new Method<>("reader", DataReader.class, omReader));
		
		omReader = new OMMethod<>("loadReader", 2, OMOperandType.POST_OPERAND);
		omReader.addOperator(new MtdLoadReader());
		methods.put("loadReader", new Method<>("loadReader", DataReader.class, omReader));
		
		omReader = new OMMethod<>("openReader", 2, OMOperandType.POST_OPERAND);
		omReader.addOperator(new MtdOpenReader());
		methods.put("openReader", new Method<>("openReader", DataReader.class, omReader));
		
		OMMethod<String> omStr = new OMMethod<>("evalString", 2, OMOperandType.POST_OPERAND);
		omStr.addOperator(new MtdEvalString());
		methods.put("evalString", new Method<>("evalString", String.class, omStr));
		
		omStr = new OMMethod<>("listToSQL", 2, OMOperandType.POST_OPERAND);
		omStr.addOperator(new MtdListToSQL());
		methods.put("listToSQL", new Method<>("listToSQL", String.class, omStr));
		
		omStr = new OMMethod<>("stringToSQL", 2, OMOperandType.POST_OPERAND);
		omStr.addOperator(new MtdStringToSQL());
		methods.put("stringToSQL", new Method<>("stringToSQL", String.class, omStr));
		
		omStr = new OMMethod<>("comaStringToSQL", 2, OMOperandType.POST_OPERAND);
		omStr.addOperator(new MtdComaStringToSQL());
		methods.put("comaStringToSQL", new Method<>("comaStringToSQL", String.class, omStr));
		
		omStr = new OMMethod<>("getStringParam", 2, OMOperandType.POST_OPERAND);
		omStr.addOperator(new MtdGetStringParam());
		methods.put("getStringParam", new Method<>("getStringParam", String.class, omStr));
		
		OMMethod<Double> omDbl = new OMMethod<>("safeDiv", 3, OMOperandType.POST_OPERAND);
		omDbl.addOperator(new MtdSafeDivider());
		methods.put("safeDiv", new Method<>("safeDiv", Double.class, omDbl));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Type<DMUtils> specificType() {
		return this;
	}
	
	public <T>void register(OMMethod<T> om, Class<T> cls) {
		methods.put(om.symbol(), new Method<>(om.symbol(), cls, om));
	}
}
