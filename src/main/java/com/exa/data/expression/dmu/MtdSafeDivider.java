package com.exa.data.expression.dmu;

import java.util.Vector;

import com.exa.data.config.utils.DMUtils;
import com.exa.expression.OMMethod;
import com.exa.expression.Type;
import com.exa.expression.XPOperand;
import com.exa.expression.OMMethod.XPOrtMethod;
import com.exa.expression.eval.ClassesMan;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;


public class MtdSafeDivider extends OMMethod.XPOrtMethod<DMUtils, Double> {
	
	public MtdSafeDivider() {
		super("safeDivider", 3);
	}
	
	@Override
	public boolean canManage(XPEvaluator eval, int order, int nbOperands) throws ManagedException {
		return true;
	}

	@Override
	public Type<?> type() {
		return ClassesMan.T_DOUBLE;
	}

	@Override
	protected XPOrtMethod<DMUtils, Double>.XPMethodResult createResultOperand(XPOperand<DMUtils> object, Vector<XPOperand<?>> params) {
		
		return new XPMethodResult(object, params) {
			
			@Override
			public Double value(XPEvaluator eval) throws ManagedException {
				
				Double num = params.get(0).asOPDouble().value(eval);
				
				Double deno = params.get(1).asOPDouble().value(eval);
				
				Double defaultValue = params.get(2).asOPDouble().value(eval);
				
				if(deno == null || deno == 0.0) return defaultValue;
				
				if(num == null) return defaultValue;
				
				return num / deno;
			}
		};
	}

}

