package com.exa.data.action;

import com.exa.data.config.utils.DMUtils;
import com.exa.expression.Type;
import com.exa.expression.Variable;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.Value;

public class ASAssignment implements ActionSeeker {
	
	private DMUtils dmu;
	
	class ACAssignment implements Action {
		private String name;
		private CalculableValue<?, XPOperand<?>> value;
		
		
		public ACAssignment(String name, CalculableValue<?, XPOperand<?>> value) {
			super();
			this.name = name;
			this.value = value;
		}
		
		@Override
		public String execute() {
			VariableContext vc = dmu.getVc();
			do {
				Variable<?> v = vc.getContextVariable(name);
				if(v != null) {
					vc.assignContextVariable(name, value.getValue());
					break;
				}
				vc = vc.getParent();
			} while(vc != null);
			if(vc == null) {
				String tn = value.typeName();
				Type<?> t = dmu.getEvaluator().getClassesMan().getType(tn);
				if(t == null) {
					System.out.println(String.format("The action type '%s' is not managed", tn));
					return "ERROR:"  + String.format("The type '%s' is not managed", tn);
				}
				
				dmu.getVc().assignOrDeclareVariable(name, t.valueClass(), value.getValue());
				
				return "OK";
			}
			
			return String.format("ERROR:No variable named '%s'", name);
			
		}
		
	}
	
	public ASAssignment(DMUtils dmu) {
		super();
		this.dmu = dmu;
	}
	
	@Override
	public Action found(String name, Value<?, XPOperand<?>> actionConfig) {
		CalculableValue<?, XPOperand<?>> cl = actionConfig.asCalculableValue();
		if(cl == null)	return null;
		
		return new ACAssignment(name, cl);
	}

}
