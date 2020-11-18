package com.exa.data.expression.dataman;

import java.util.Vector;

import com.exa.data.DataReader;
import com.exa.expression.OMMethod;
import com.exa.expression.Type;
import com.exa.expression.XPOperand;
import com.exa.expression.OMMethod.XPOrtMethod;
import com.exa.expression.eval.ClassesMan;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;

public class MtdDrGetInteger extends OMMethod.XPOrtMethod<DataReader<?>, Integer> {

	public MtdDrGetInteger() {
		super("getInteger", 1);
	}

	@Override
	public boolean canManage(XPEvaluator arg0, int arg1, int arg2) throws ManagedException {
		return true;
	}

	@Override
	public Type<?> type() {
		return ClassesMan.T_INTEGER;
	}

	@Override
	protected XPOrtMethod<DataReader<?>, Integer>.XPMethodResult createResultOperand(XPOperand<DataReader<?>> xpDR, Vector<XPOperand<?>> xpFieldName) {
		
		return new XPMethodResult(xpDR, xpFieldName) {
			
			@Override
			public Integer value(XPEvaluator eval) throws ManagedException {
				String fieldName = xpFieldName.get(0).asOPString().value(eval);
				DataReader<?> dr = xpDR.value(eval);
				
				//if(DMUtils.FIELD_DEBUG) System.out.println(String.format("Getting Integer field '%s'", fieldName));
				
				return dr.getInteger(fieldName);
			}
		};
	}

}
