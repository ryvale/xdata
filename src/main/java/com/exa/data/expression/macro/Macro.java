package com.exa.data.expression.macro;

import com.exa.data.config.utils.DMutils;
import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;

public abstract class Macro<T> {
	
	protected DMutils dmu;
	
	public Macro(DMutils dmu) {
		super();
		this.dmu = dmu;
	}

	public Macro<String> asMacroString() { return null; }
	
	public abstract T value(String macroName, ObjectValue<XPOperand<?>> ovMacro) throws ManagedException;

}
