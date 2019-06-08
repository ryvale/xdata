package com.exa.data.expression;

import com.exa.expression.eval.ClassesMan;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.expression.VIEvaluatorSetup;
import com.exa.utils.ManagedException;

public class DCEvaluatorSetup extends VIEvaluatorSetup {

	@Override
	public void setup(XPEvaluator evaluator) throws ManagedException {
		ClassesMan cm = evaluator.getClassesMan();
		
		cm.registerClass(new TDataReader());
		cm.registerClass(new TDataWriter());
		
		super.setup(evaluator);
	}
	
}
