package com.exa.data;

import java.util.LinkedHashMap;
import java.util.Map;

import com.exa.data.config.utils.DMUtils;
import com.exa.expression.eval.XPEvaluator;

public abstract class StandardDataWriterBase<_FIELD extends Field> implements DataWriter<_FIELD> {
	protected Map<String, _FIELD> fields = new LinkedHashMap<>();
	
	//protected XPEvaluator evaluator = null;
	
	protected DataMan parent;
	
	protected String name;
	
	//protected VariableContext variableContext;
	
	protected DataReader<?> drSource;
	
	protected DMUtils dmu;
	
	public StandardDataWriterBase(String name, DataReader<?> drSource/*, XPEvaluator evaluator, VariableContext variableContext*/, DMUtils dmu) {
		super();
		this.name = name;
		
		this.drSource = drSource;
		
		/*this.evaluator = evaluator;
		this.variableContext = variableContext;*/
		this.dmu = dmu;
	}

	/*@Override
	public void setEvaluator(XPEvaluator evaluator) {
		this.evaluator = evaluator;
	}*/

	@Override
	public XPEvaluator getEvaluator() {	return dmu.getEvaluator(); }

	@Override
	public DataReader<?> asDataReader() { return null; }

	@Override
	public DataWriter<?> asDataWriter() {
		return this;
	}
	
	@Override
	public boolean containsField(String fieldName) {
		return fields.containsKey(fieldName);
	}

	@Override
	public boolean execute() throws DataException {
		update(drSource);
		
		return true;
	}
	
}
