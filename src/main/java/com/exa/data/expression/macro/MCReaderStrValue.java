package com.exa.data.expression.macro;

import com.exa.data.DataReader;
import com.exa.data.config.DMFGeneral;
import com.exa.data.config.DataManFactory;
import com.exa.data.config.utils.DMUtils;
import com.exa.expression.XPOperand;
import com.exa.lang.parsing.Computing;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;

public class MCReaderStrValue extends Macro<String> {
	
	public MCReaderStrValue(DMUtils dmu) {
		super(dmu);
	}
	
	@Override
	public Macro<String> asMacroString() {
		return this;
	}

	@Override
	public String value(String macroName, ObjectValue<XPOperand<?>> ovMacro) throws ManagedException {
		String fieldName = ovMacro.getRequiredAttributAsString("field");
		
		ObjectValue<XPOperand<?>> ovReader = Computing.object(DataManFactory.parser, ovMacro.getRequiredAttributAsObjectValue("reader"), dmu.getEvaluator(), dmu.getVc(), Computing.getDefaultObjectLib(dmu.getOvRoot()));
		
		DMFGeneral dmf = dmu.getDmf();
		
		DMUtils subDmu = dmu.newSubDmu(ovReader.getAttributAsString("dataSource", dmf.getDefaultDataSource()));
		DataReader<?> dr = dmf.getDataReader("macro:" + macroName, ovReader, subDmu);
		
		try {
			dr.open();dr.next();
			return dr.getString(fieldName);
		}
		finally {
			try { dr.close(); } catch (Exception e) { e.printStackTrace(); }
		}
	}
 
}
