package com.exa.data;

import java.util.LinkedHashMap;
import java.util.Map;

import com.exa.expression.VariableContext;
import com.exa.expression.eval.XPEvaluator;

public abstract class StandardDataWriterBase<_FIELD extends Field> implements DataWriter<_FIELD> {
	protected Map<String, _FIELD> fields = new LinkedHashMap<>();
	
	protected XPEvaluator evaluator = null;
	
	protected DataMan parent;
	
	protected String name;
	
	protected VariableContext variableContext;
	
	protected DataReader<?> drSource;
	
	public StandardDataWriterBase(String name, DataReader<?> drSource, XPEvaluator evaluator, VariableContext variableContext) {
		super();
		this.name = name;
		
		this.drSource = drSource;
		
		this.evaluator = evaluator;
		this.variableContext = variableContext;
	}

	@Override
	public void setEvaluator(XPEvaluator evaluator) {
		this.evaluator = evaluator;
	}

	@Override
	public XPEvaluator getEvaluator() {	return evaluator; }

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
