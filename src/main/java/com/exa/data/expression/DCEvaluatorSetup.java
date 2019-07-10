package com.exa.data.expression;

import com.exa.expression.eval.ClassesMan;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.expression.VIEvaluatorSetup;
import com.exa.utils.ManagedException;

public class DCEvaluatorSetup extends VIEvaluatorSetup {
	
	public final static TDataReader T_DATA_READER = new TDataReader();

	@Override
	public void setup(XPEvaluator evaluator) throws ManagedException {
		ClassesMan cm = evaluator.getClassesMan();
		
		cm.registerClass(T_DATA_READER);
		cm.registerClass(new TDataWriter());
		cm.registerClass(new TDMUtils());
		
		//new XPOprtMemberAccess<>(".", T_DATA_READER);
		
		//evaluator.addMemberAccessOprt(new XPOprtMemberAccess<>(".", T_DATA_READER));
		
		super.setup(evaluator);
	}
	
}
