package com.exa.data.expression;

import java.util.Date;

import com.exa.data.DataMan;
import com.exa.data.DataReader;
import com.exa.eva.OperatorManager.OMOperandType;
import com.exa.expression.OMMethod;
import com.exa.expression.Type;
import com.exa.expression.types.TObjectClass;

public class TDataReader extends TObjectClass<DataReader<?>, DataMan> {

	public TDataReader() {
		super(null, DataReader.class, "DataReader");
	}

	@Override
	public void initialize() {
		OMMethod<String> omStr = new OMMethod<>("getString", 2, OMOperandType.POST_OPERAND);
		omStr.addOperator(new MethodGetString());
		methods.put("getString", new Method<>("getString", String.class, omStr));
		
		OMMethod<Date> omDate = new OMMethod<>("getDate", 2, OMOperandType.POST_OPERAND);
		omDate.addOperator(new MethodGetDate());
		methods.put("getDate", new Method<>("getDate", Date.class, omDate));
		
		OMMethod<Double> omDbl = new OMMethod<>("getDouble", 2, OMOperandType.POST_OPERAND);
		omDbl.addOperator(new MethodGetDouble());
		methods.put("getDouble", new Method<>("getDouble", Double.class, omDbl));
		
		properties.put("lineVisited", new Property<>("lineVisited", Integer.class, object -> object.lineVisited()));
	}

	@Override
	public Type<DataReader<?>> specificType() {
		return this;
	}
	
	
	
}
