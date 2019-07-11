package com.exa.data.expression.macro;

import com.exa.utils.ManagedException;

public abstract class Macro<T> {
	
	public abstract String typeName();
	
	
	public abstract T value() throws ManagedException;

}
