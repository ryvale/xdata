package com.exa.data.expression.dmu;

import com.exa.data.DataReader;
import com.exa.data.config.utils.DMutils;
import com.exa.eva.OperatorManager.OMOperandType;
import com.exa.expression.OMMethod;
import com.exa.expression.Type;

import com.exa.expression.types.TObjectClass;

public class TDMUtils extends TObjectClass<DMutils, Object> {

	public TDMUtils() {
		super(null, DMutils.class, "DMUtils");
	}
	
	@Override
	public void initialize() {
		@SuppressWarnings("rawtypes")
		OMMethod<DataReader> omReader = new OMMethod<>("reader", 2, OMOperandType.POST_OPERAND);
		omReader.addOperator(new MtdReader());
		
		methods.put("reader", new Method<>("reader", DataReader.class, omReader));
		
		OMMethod<String> omStr = new OMMethod<>("evalString", 2, OMOperandType.POST_OPERAND);
		omStr.addOperator(new MtdEvalString());
		methods.put("evalString", new Method<>("evalString", String.class, omStr));
	}



	@SuppressWarnings("unchecked")
	@Override
	public Type<DMutils> specificType() {
		return this;
	}
}
