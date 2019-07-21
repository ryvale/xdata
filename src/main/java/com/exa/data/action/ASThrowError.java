package com.exa.data.action;

import com.exa.expression.XPOperand;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;

public class ASThrowError implements ActionSeeker {
	
	class ACThrowError implements Action {
		private String errMessage;
		
		public ACThrowError(String errMessage) {
			super();
			this.errMessage = errMessage;
		}
		@Override
		public String execute() {
			return "ERROR:"+ errMessage;
		}
		
	}

	@Override
	public Action found(String name, Value<?, XPOperand<?>> actionConfig) {
		ObjectValue<XPOperand<?>> ovAction = actionConfig.asObjectValue();
		if(ovAction == null) return null;
		
		Value<?, XPOperand<?>> vlType = ovAction.getAttribut("type");
		if(vlType == null) return null;
		
		StringValue<XPOperand<?>> svType = vlType.asStringValue();
		if(svType == null) return null;
		
		if(!"throw-error".equals(svType.getValue())) return null;
		
		
		//if("throw-error".equals(ovAction.getAtt))
		
		return null;
	}

}
