package com.exa.data;

import java.util.LinkedHashMap;
import java.util.Map;

import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.expression.XALCalculabeValue;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public abstract class StandardDataReaderBase<_FIELD extends Field> implements DataReader<_FIELD> {
	protected Map<String, _FIELD> fields = new LinkedHashMap<>();
	
	protected XPEvaluator evaluator = null;
	
	protected DataReader<?> parent;
	
	protected String name;
	
	protected VariableContext variableContext;
	
	public StandardDataReaderBase(String name, XPEvaluator evaluator, VariableContext variableContext) {
		super();
		this.name = name;
		
		this.evaluator = evaluator;
		this.variableContext = variableContext;
	}
	
	/*public StandardDataReaderBase(String name, VariableContext variableContext) {
		this(name, null);
	}*/

	@Override
	public boolean execute() throws DataException {
		return next();
	}

	@Override
	public void executeFieldsAction(FieldAction<_FIELD> fa) throws DataException {
		
		for(_FIELD field : fields.values()) {
			fa.execute(field);
		}
	}

	@Override
	public _FIELD getField(String name) {
		return fields.get(name);
	}

	@Override
	public boolean containsField(String fieldName) {
		return fields.containsKey(fieldName);
	}
	
	@Override
	public abstract StandardDataReaderBase<_FIELD> cloneDR() throws DataException;
	
	@Override
	public DataReader<?> getSubDataReader(String name) {
		return null;
	}

	@Override
	public Object getObject(String fieldName) throws DataException {
		_FIELD field = fields.get(fieldName);
		if(field == null) throw new DataException(String.format("Unknown field name %s", fieldName));
		
		if("date".equals(field.getType()) || "datetime".equals(field.getType())) return getDate(fieldName);
		
		if("double".equals(field.getType()) || "float".equals(field.getType()) || "decimal".equals(field.getType())) return getDouble(fieldName);
		
		return getString(fieldName);
	}
	
	/*@Override
	public int lineVisited() {
		return _lineVisited;
	}*/

	@Override
	public XPEvaluator getEvaluator() {
		return evaluator;
	}

	@Override
	public void setEvaluator(XPEvaluator evaluator) {
		this.evaluator = evaluator;
	}

	@Override
	public DataReader<?> getParent() {
		return parent;
	}

	@Override
	public void setParent(DataReader<?> parent) {
		this.parent = parent;
	}

	@Override
	public DataReader<?> asDataReader() {
		return this;
	}

	public VariableContext getVariableContext() {
		return variableContext;
	}

	public void setVariableContext(VariableContext variableContext) {
		this.variableContext = variableContext;
	}
	
	public static void updateVariableContext(ObjectValue<XPOperand<?>> ov, VariableContext vc, VariableContext prentVC) {
		Map<String, Value<?, XPOperand<?>>> mp = ov.getValue();
		
		for(String propertyName : mp.keySet()) {
			Value<?, XPOperand<?>> vl=mp.get(propertyName);
			
			ObjectValue<XPOperand<?>> vov = vl.asObjectValue();
			if(vov != null) {
				updateVariableContext(vov, vc, prentVC);
				continue;
			}
			
			CalculableValue<?, XPOperand<?>> cl = vl.asCalculableValue();
			if(cl == null) continue;
			
			XALCalculabeValue<?> xalCL = (XALCalculabeValue<?>) cl;
			if(xalCL.getVariableContext() == prentVC) xalCL.setVariableContext(vc);
			
		}
	}
}
