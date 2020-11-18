package com.exa.data.expression.dmu;

import java.util.Vector;

import com.exa.data.DataReader;
import com.exa.data.config.utils.DMUtils;
import com.exa.data.expression.DCEvaluatorSetup;
import com.exa.expression.OMMethod;
import com.exa.expression.OMMethod.XPOrtMethod;

import com.exa.expression.Type;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;

@SuppressWarnings("rawtypes")
public class MtdLoadReader extends OMMethod.XPOrtMethod<DMUtils, DataReader> {

	public MtdLoadReader() {
		super("loadReader", 2);
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
	protected XPOrtMethod<DMUtils, DataReader>.XPMethodResult createResultOperand(XPOperand<DMUtils> object, Vector<XPOperand<?>> params) {
		return new XPMethodResult(object, params) {
			
			@Override
			public DataReader<?> value(XPEvaluator eval) throws ManagedException {
				String name = params.get(0).asOPString().value(eval);
				
				DMUtils v = object.value(eval);
				
				return v.loadReader(name);
			}
		};
	}

}
