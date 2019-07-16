package com.exa.data.expression;

import com.exa.data.expression.dataman.TDataReader;
import com.exa.data.expression.dataman.TDataWriter;
import com.exa.data.expression.dmu.TDMUtils;
import com.exa.expression.eval.ClassesMan;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.expression.VIEvaluatorSetup;
import com.exa.utils.ManagedException;

public class DCEvaluatorSetup extends VIEvaluatorSetup {
	
	public final static TDataReader T_DATA_READER = new TDataReader();
	
	public final static TDataWriter T_DATA_WRITER = new TDataWriter();
	
	public final static TDMUtils T_DMU = new TDMUtils();

	@Override
	public void setup(XPEvaluator evaluator) throws ManagedException {
		ClassesMan cm = evaluator.getClassesMan();
		
		cm.registerClass(T_DATA_READER);
		cm.registerClass(T_DATA_WRITER);
		cm.registerClass(T_DMU);
		
		super.setup(evaluator);
	}
	
}
