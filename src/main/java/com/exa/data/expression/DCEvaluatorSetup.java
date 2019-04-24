package com.exa.data.expression;

import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.expression.VIEvaluatorSetup;
import com.exa.utils.ManagedException;

public class DCEvaluatorSetup extends VIEvaluatorSetup {

	@Override
	public void setup(XPEvaluator evaluator) throws ManagedException {
		evaluator.getClassesMan().registerClass(new TDataReader());
		
		super.setup(evaluator);
	}
	
}
