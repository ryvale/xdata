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

public class MtdDrDataInBuffer extends OMMethod.XPOrtMethod<DataReader<?>, Boolean> {

	public MtdDrDataInBuffer() {
		super("dataInBuffer", 1);
	}

	@Override
	public boolean canManage(XPEvaluator eval, int order, int nbOperands) throws ManagedException {
		return true;
	}

	@Override
	public Type<?> type() {
		return ClassesMan.T_BOOLEAN;
	}

	@Override
	protected XPOrtMethod<DataReader<?>, Boolean>.XPMethodResult createResultOperand(XPOperand<DataReader<?>> object,
			Vector<XPOperand<?>> params) {

		return new XPMethodResult(object, params) {
			
			@Override
			public Boolean value(XPEvaluator eval) throws ManagedException {
				DataReader<?> dm = object.value(eval);
				
				return dm.dataInBuffer();
			}
		};
	}

	

}
