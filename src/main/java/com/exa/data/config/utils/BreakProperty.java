package com.exa.data.config.utils;

import com.exa.data.DataException;
import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public class BreakProperty {
	
	private Value<?, XPOperand<?>> vlCondition;
	
	private Value<?, XPOperand<?>> vlThrowError;
	
	private Value<?, XPOperand<?>> vlUserMessage;

	public BreakProperty(Value<?, XPOperand<?>> vlCondition, Value<?, XPOperand<?>> vlThrowError, Value<?, XPOperand<?>> vlUserMessage) {
		super();
		this.vlCondition = vlCondition;
		this.vlThrowError = vlThrowError;
		this.vlUserMessage = vlUserMessage;
	}

	public Value<?, XPOperand<?>> getVlCondition() {
		return vlCondition;
	}

	public void setVlCondition(Value<?, XPOperand<?>> vlCondition) {
		this.vlCondition = vlCondition;
	}

	public Value<?, XPOperand<?>> getVlThrowError() {
		return vlThrowError;
	}

	public void setVlThrowError(Value<?, XPOperand<?>> vlThrowError) {
		this.vlThrowError = vlThrowError;
	}

	public Value<?, XPOperand<?>> getVlUserMessage() {
		return vlUserMessage;
	}

	public void setVlUserMessage(Value<?, XPOperand<?>> vlUserMessage) {
		this.vlUserMessage = vlUserMessage;
	}
	
	public static BreakProperty parseBreakItemConfig(Value<?, XPOperand<?>> vlBreak, String dataManName) throws ManagedException {
		ObjectValue<XPOperand<?>> ovBreak = vlBreak.asObjectValue();
		if(ovBreak != null) {
			vlBreak = ovBreak.getRequiredAttribut("condition");
			if(!"boolean".equals(vlBreak.typeName())) throw new DataException(String.format("The property 'break.condtion' should be a boolean in data reader named '%'", dataManName));
			
			Value<?, XPOperand<?>> vlBreakUserMessage = ovBreak.getAttribut("userMessage");
			if(vlBreakUserMessage != null)
				if(!"string".equals(vlBreakUserMessage.typeName())) throw new DataException(String.format("The property 'break.userMessage' should be a string in data reader named '%'", dataManName));
			
			Value<?, XPOperand<?>>vlBreakThrowError = ovBreak.getAttribut("throwError");
			if(vlBreakThrowError != null) {
				if(!"string".equals(vlBreakThrowError.typeName())) throw new DataException(String.format("The property 'break.throwError' should be a string in data reader named '%'", dataManName));
			}
			else vlBreakThrowError = vlBreakUserMessage;
			
			return new BreakProperty(vlBreak, vlBreakThrowError, vlBreakUserMessage);
		}
		
		if(!"boolean".equals(vlBreak.typeName())) throw new DataException(String.format("The break property should be a boolean or object in data reader named '%'", dataManName));
		
		return new BreakProperty(vlBreak, null, null);
	}


}
