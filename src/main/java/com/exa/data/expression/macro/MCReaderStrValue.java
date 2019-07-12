package com.exa.data.expression.macro;

import com.exa.data.DataReader;
import com.exa.data.config.utils.DMutils;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.parsing.Computing;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;

public class MCReaderStrValue extends Macro<String> {
	
	public MCReaderStrValue(DMutils dmu) {
		super(dmu);
	}
	
	@Override
	public Macro<String> asMacroString() {
		return this;
	}

	@Override
	public String value(String macroName, ObjectValue<XPOperand<?>> ovMacro) throws ManagedException {
		String fieldName = ovMacro.getRequiredAttributAsString("field");
		
		ObjectValue<XPOperand<?>> ovReader = Computing.object(dmu.getParser(), ovMacro.getRequiredAttributAsObjectValue("reader"), dmu.getEvaluator(), dmu.getVc(), Computing.getDefaultObjectLib(dmu.getOvRoot()));
		
		DataReader<?> dr = dmu.getDmf().getDataReader("macro:" + macroName, ovReader, dmu.getEvaluator(), dmu.getVc(), dmu);
		
		try {
			dr.open();
			return dr.getString(fieldName);
		}
		finally {
			try { dr.close(); } catch (Exception e) { e.printStackTrace(); }
		}
	}
 
}
