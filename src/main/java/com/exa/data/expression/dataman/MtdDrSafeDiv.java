package com.exa.data.expression.dataman;

import java.util.Vector;

import com.exa.data.DataReader;
import com.exa.expression.OMMethod;
import com.exa.expression.OMMethod.XPOrtMethod;

import com.exa.expression.Type;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.ClassesMan;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;

public class MtdDrSafeDiv extends OMMethod.XPOrtMethod<DataReader<?>, Double> {

	public MtdDrSafeDiv() {
		super("safeDiv", 3);
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
	protected XPOrtMethod<DataReader<?>, Double>.XPMethodResult createResultOperand(XPOperand<DataReader<?>> xpDR, Vector<XPOperand<?>> xpFieldName) {
		
		return new XPMethodResult(xpDR, xpFieldName) {
			
			@Override
			public Double value(XPEvaluator eval) throws ManagedException {
				String numFieldName = xpFieldName.get(0).asOPString().value(eval);
				
				String denoFieldName = xpFieldName.get(1).asOPString().value(eval);
				
				Double defaultValue = xpFieldName.get(2).asOPDouble().value(eval);
				
				
				DataReader<?> dr = xpDR.value(eval);
				
				Double num = dr.getDouble(numFieldName);
				
				if(num == null) return defaultValue;
				
				Double deno = dr.getDouble(denoFieldName);
				
				if(deno == null || deno == 0.0) return defaultValue;
				
				
				return num / deno;
			}
		};
	}
	
	
}
