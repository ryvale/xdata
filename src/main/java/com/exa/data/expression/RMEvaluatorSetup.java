package com.exa.data.expression;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.exa.data.expression.XDataConfig;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.parsing.Computing.EvaluatorSetup;
import com.exa.utils.ManagedException;

public class RMEvaluatorSetup implements EvaluatorSetup {
	public static class ValueInfo<T> {
		private Class<?> valueClass;
		private T value;
		
		public ValueInfo(Class<?> valueClass, T value) {
			super();
			this.valueClass = valueClass;
			this.value = value;
		}

		public Class<?> valueClass() { return valueClass; }

		public T value() { return value; }
	}
	
	private Map<String, ValueInfo<?>> managedVariables = new HashMap<>();

	@Override
	public void setup(XPEvaluator evaluator) throws ManagedException {
		evaluator.getClassesMan().registerClass(new TGlobalParams());
		
		XDataConfig.setup(evaluator.getClassesMan());
		
		injectVariables(evaluator);
	}
	
	public void injectVariables(XPEvaluator evaluator) throws ManagedException {
		Map<String, Object> mpGP = new LinkedHashMap<>();
		GlobalParams gp = new GlobalParams(mpGP);
		
		evaluator.addVariable("params", gp.getClass(), gp);
		for(String varName : managedVariables.keySet()) {
			ValueInfo<?> value = managedVariables.get(varName);
			if("params".equals(varName)) continue;
			evaluator.assignOrDeclareVariable(varName, value.valueClass, value.value);
			
			mpGP.put(varName, value.value);
		}
		
	}
	
	public <T>void addVaraiable(String varName, Class<?> valueClass, T value) {
		managedVariables.put(varName, new ValueInfo<T>(valueClass, value));
	}
	
	public void disposeVariable(String varName) {
		managedVariables.remove(varName);
	}
	
}
