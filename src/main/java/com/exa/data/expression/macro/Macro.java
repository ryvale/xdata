package com.exa.data.expression.macro;

import com.exa.data.DataReader;
import com.exa.data.config.utils.DMUtils;
import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;

public abstract class Macro<T> {
	
	protected DMUtils dmu;
	
	public Macro(DMUtils dmu) {
		super();
		this.dmu = dmu;
	}

	public Macro<String> asMacroString() { return null; }
	
	public Macro<DataReader<?>> asMacroDataReader() { return null; }
	
	public abstract T value(String macroName, ObjectValue<XPOperand<?>> ovMacro) throws ManagedException;

}
