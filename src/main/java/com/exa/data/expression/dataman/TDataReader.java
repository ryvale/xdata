package com.exa.data.expression.dataman;

import java.util.Date;

import com.exa.data.DataMan;
import com.exa.data.DataReader;
import com.exa.eva.OperatorManager.OMOperandType;
import com.exa.expression.OMMethod;
import com.exa.expression.Type;
import com.exa.expression.types.TObjectClass;
import com.exa.utils.ManagedException;

public class TDataReader extends TObjectClass<DataReader<?>, DataMan> {

	public TDataReader() {
		super(null, DataReader.class, "DataReader");
	}

	@Override
	public void initialize() {
		OMMethod<String> omStr = new OMMethod<>("getString", 2, OMOperandType.POST_OPERAND);
		omStr.addOperator(new MtdDrGetString());
		methods.put("getString", new Method<>("getString", String.class, omStr));
		
		OMMethod<Date> omDate = new OMMethod<>("getDate", 2, OMOperandType.POST_OPERAND);
		omDate.addOperator(new MtdDrGetDate());
		methods.put("getDate", new Method<>("getDate", Date.class, omDate));
		
		OMMethod<Double> omDbl = new OMMethod<>("getDouble", 2, OMOperandType.POST_OPERAND);
		omDbl.addOperator(new MtdDrGetDouble());
		methods.put("getDouble", new Method<>("getDouble", Double.class, omDbl));
		
		omDbl = new OMMethod<>("getXDouble", 3, OMOperandType.POST_OPERAND);
		omDbl.addOperator(new MtdDrGetXDouble());
		methods.put("getXDouble", new Method<>("getXDouble", Double.class, omDbl));
		
		omDbl = new OMMethod<>("safeDiv", 4, OMOperandType.POST_OPERAND);
		omDbl.addOperator(new MtdDrSafeDiv());
		methods.put("safeDiv", new Method<>("safeDivj", Double.class, omDbl));
		
		OMMethod<Integer> omInt = new OMMethod<>("getInteger", 2, OMOperandType.POST_OPERAND);
		omInt.addOperator(new MtdDrGetInteger());
		methods.put("getInteger", new Method<>("getInteger", Integer.class, omInt));
		
		OMMethod<Boolean> omBl = new OMMethod<>("execute", 1, OMOperandType.POST_OPERAND);
		omBl.addOperator(new MtdExecute());
		methods.put("execute", new Method<>("execute", Boolean.class, omBl));
		
		omBl = new OMMethod<>("next", 1, OMOperandType.POST_OPERAND);
		omBl.addOperator(new MtdDrNext());
		methods.put("next", new Method<>("next", Boolean.class, omBl));
		
		omBl = new OMMethod<>("close", 1, OMOperandType.POST_OPERAND);
		omBl.addOperator(new MtdDrClose());
		methods.put("close", new Method<>("close", Boolean.class, omBl));
		
		properties.put("lineVisited", new Property<>("lineVisited", Integer.class, object -> object.lineVisited()));
		
		properties.put("dataInBuffer", new Property<>("dataInBuffer", Boolean.class, object -> object.dataInBuffer()));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Type<DataReader<?>> specificType() {
		return this;
	}

	@Override
	public DataReader<?> convert(Object o) throws ManagedException {
		if(o instanceof DataReader) return (DataReader<?>) o;
		
		return super.convert(o);
	}
	
	
	
}
