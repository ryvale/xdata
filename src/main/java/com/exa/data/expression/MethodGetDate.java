package com.exa.data.expression;

import java.util.Date;
import java.util.Vector;

import com.exa.data.DataReader;
import com.exa.expression.OMMethod;
import com.exa.expression.OMMethod.XPOrtMethod;
import com.exa.expression.Type;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.ClassesMan;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;

public class MethodGetDate extends OMMethod.XPOrtMethod<DataReader<?>, Date> {

	public MethodGetDate() {
		super("getDate", 2);
	}

	@Override
	public boolean canManage(XPEvaluator arg0, int arg1, int arg2) throws ManagedException {
		return true;
	}

	@Override
	public Type<?> type() {
		return ClassesMan.T_DATE;
	}

	@Override
	protected XPOrtMethod<DataReader<?>, Date>.XPMethodResult createResultOperand(XPOperand<DataReader<?>> xpDR, Vector<XPOperand<?>> xpFieldName) {
		
		return new XPMethodResult(xpDR, xpFieldName) {
			
			@Override
			public Date value(XPEvaluator eval) throws ManagedException {
				String fieldName = xpFieldName.get(0).asOPString().value(eval);
				DataReader<?> dr = xpDR.value(eval);
				
				return dr.getDate(fieldName);
			}
		};
	}

}
