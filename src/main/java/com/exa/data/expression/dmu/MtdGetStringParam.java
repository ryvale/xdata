package com.exa.data.expression.dmu;

import java.util.Vector;

import com.exa.data.config.utils.DMUtils;
import com.exa.expression.OMMethod;
import com.exa.expression.OMMethod.XPOrtMethod;
import com.exa.expression.Type;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.ClassesMan;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;

public class MtdGetStringParam extends OMMethod.XPOrtMethod<DMUtils, String> {

	public MtdGetStringParam() {
		super("getStringParam", 2);
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
	protected XPOrtMethod<DMUtils, String>.XPMethodResult createResultOperand(XPOperand<DMUtils> object, Vector<XPOperand<?>> params) {
		return new XPMethodResult(object, params) {
			
			@Override
			public String value(XPEvaluator evaluator) throws ManagedException {
				String paramName = params.get(0).asOPString().value(evaluator);
				
				DMUtils dmu = object.value(evaluator);
				
				Object ob = dmu.getParams().get(paramName);
				
				if(ob == null) return null;
				
				return (String)ob;
			}
		};
	}

}
