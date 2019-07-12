package com.exa.data.expression.dmu;

import java.util.Vector;

import com.exa.data.config.utils.DMutils;
import com.exa.expression.OMMethod;
import com.exa.expression.OMMethod.XPOrtMethod;

import com.exa.expression.Type;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.ClassesMan;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;

public class MtdEvalString extends OMMethod.XPOrtMethod<DMutils, String> {

	public MtdEvalString() {
		super("evalString", 2);
	}

	@Override
	public boolean canManage(XPEvaluator eval, int order, int nbOperands) throws ManagedException {
		return true;
	}

	@Override
	public Type<?> type() {
		return ClassesMan.T_STRING;
	}

	@Override
	protected XPOrtMethod<DMutils, String>.XPMethodResult createResultOperand(XPOperand<DMutils> object, Vector<XPOperand<?>> params) {
		return new XPMethodResult(object, params) {
			
			@Override
			public String value(XPEvaluator evaluator) throws ManagedException {
				String macroRef = params.get(0).asOPString().value(evaluator);
				
				DMutils v = object.value(evaluator);
				
				return v.evalString(macroRef);
			}
		};
	}

}
