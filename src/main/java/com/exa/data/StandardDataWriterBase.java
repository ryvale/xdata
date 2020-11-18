package com.exa.data;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exa.data.config.utils.DMUtils;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.ManagedException;

public abstract class StandardDataWriterBase<_FIELD extends Field> implements DataWriter<_FIELD> {
	public static boolean debugOn = false;
	
	public static final Logger LOG = LoggerFactory.getLogger(StandardDataWriterBase.class);
	
	protected Map<String, _FIELD> fields = new LinkedHashMap<>();
	
	protected DataMan parent;
	
	protected String name;
	
	protected DataReader<?> drSource;
	
	protected DMUtils dmu;
	
	public StandardDataWriterBase(String name, DataReader<?> drSource, DMUtils dmu) {
		super();
		this.name = name;
		
		this.drSource = drSource;
		
		this.dmu = dmu;
	}
	
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
		if(debugOn) {
			LOG.info(String.format("START:'OnExecutionStarted' for writer '%s'", name));
		}
		try {
			dmu.executeOnExecutionStarted();
			if(debugOn) {
				LOG.info(String.format("OK:'OnExecutionStarted' for writer '%s'", name));
			}
		} catch (ManagedException e) {
			LOG.warn(String.format("FAIL:'OnExecutionStarted' for writer '%s : %s'", name, e.getMessage()));
			if(e instanceof DataException) throw (DataException)e;
			throw new DataException(e);
		}
		int r = update(drSource);
		
		return r > 0;
	}
	
}
