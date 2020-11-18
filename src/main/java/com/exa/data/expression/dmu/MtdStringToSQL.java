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

public class MtdStringToSQL extends OMMethod.XPOrtMethod<DMUtils, String> {

	public MtdStringToSQL() {
		super("stringToSQL", 1);
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
			public String value(XPEvaluator eval) throws ManagedException {
				
				String p1 = params.get(0).asOPString().value(eval);
				
				return DMUtils.toSQLString(p1, "null");
			}
		};
	}

}
