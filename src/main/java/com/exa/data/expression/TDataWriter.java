package com.exa.data.expression;

import com.exa.data.DataMan;
import com.exa.data.DataWriter;
import com.exa.eva.OperatorManager.OMOperandType;
import com.exa.expression.OMMethod;
import com.exa.expression.Type;
import com.exa.expression.types.TObjectClass;

public class TDataWriter extends TObjectClass<DataWriter<?>, DataMan> {

	public TDataWriter() {
		super(null, DataWriter.class, "DataWriter");	
	}
	
	@Override
	public void initialize() {
		OMMethod<Boolean> omBl = new OMMethod<>("execute", 1, OMOperandType.POST_OPERAND);
		omBl.addOperator(new MethodExecute());
		methods.put("execute", new Method<>("execute", Boolean.class, omBl));
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Type<DataWriter<?>> specificType() {
		return this;
	}

}
