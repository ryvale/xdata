package com.exa.data.expression.dmu;

import java.util.Vector;

import com.exa.data.DataReader;
import com.exa.data.config.utils.DMutils;
import com.exa.data.expression.DCEvaluatorSetup;
import com.exa.expression.OMMethod;
import com.exa.expression.OMMethod.XPOrtMethod;
import com.exa.expression.Type;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;

@SuppressWarnings("rawtypes")
public class MtdReader extends OMMethod.XPOrtMethod<DMutils, DataReader> {

	public MtdReader() {
		super("reader", 2);
	}

	@Override
	public boolean canManage(XPEvaluator eval, int order, int nbOperands) throws ManagedException {
		return true;
	}

	@Override
	public Type<?> type() {
		return DCEvaluatorSetup.T_DATA_READER;
	}

	@Override
	protected XPOrtMethod<DMutils, DataReader>.XPMethodResult createResultOperand(XPOperand<DMutils> object, Vector<XPOperand<?>> params) {
		return new XPMethodResult(object, params) {
			
			@Override
			public DataReader<?> value(XPEvaluator eval) throws ManagedException {
				String name = params.get(0).asOPString().value(eval);
				
				DMutils v = object.value(eval);
				
				return v.getReader(name);
			}
		};
	}

}
