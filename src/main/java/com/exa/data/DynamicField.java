package com.exa.data;

import com.exa.expression.XPOperand;
import com.exa.utils.values.Value;

public class DynamicField extends Field {
	
	private Value<?, XPOperand<?>> vlName;
	private String expType;
	private Value<?, XPOperand<?>> vlCondition;
	
	//private Value<?, XPOperand<?>> fromString;
	
	private Value<?, XPOperand<?>> from = null;
	
	private Value<?, XPOperand<?>> fromFormat = null;
	
	private Value<?, XPOperand<?>> vlExp;
	
	public DynamicField(String name, String type, String expType) {
		super(name, type);
		this.expType = expType;
	}
	
	public DynamicField(String name, String valueType) {
		super(name, "string");
	}

	public String getExpType() {
		return expType;
	}

	public void setExpType(String expType) {
		this.expType = expType;
	}

	public Value<?, XPOperand<?>> getVlExp() {
		return vlExp;
	}

	public void setVlExp(Value<?, XPOperand<?>> vlExp) {
		this.vlExp = vlExp;
	}

	public Value<?, XPOperand<?>> getVlCondition() {
		return vlCondition;
	}

	public void setVlCondition(Value<?, XPOperand<?>> vlCondition) {
		this.vlCondition = vlCondition;
	}

	public Value<?, XPOperand<?>> getVlName() {
		return vlName;
	}

	public void setVlName(Value<?, XPOperand<?>> vlName) {
		this.vlName = vlName;
	}

	/*public Value<?, XPOperand<?>> getFromString() {
		return fromString;
	}

	public void setFromString(Value<?, XPOperand<?>> fromString) {
		this.fromString = fromString;
	}*/

	public Value<?, XPOperand<?>> getFrom() {
		return from;
	}

	public void setFrom(Value<?, XPOperand<?>> from) {
		this.from = from;
	}

	public Value<?, XPOperand<?>> getFromFormat() {
		return fromFormat;
	}

	public void setFromFormat(Value<?, XPOperand<?>> fromFormat) {
		this.fromFormat = fromFormat;
	}
	
	
}
