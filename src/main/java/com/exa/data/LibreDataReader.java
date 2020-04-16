package com.exa.data;

import java.util.Date;

import com.exa.data.config.utils.DMUtils;
import com.exa.expression.XPOperand;

import com.exa.lang.expression.XALCalculabeValue;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public class LibreDataReader extends StandardDataReaderBase<Field> {
	private ObjectValue<XPOperand<?>> config;
	
	protected Integer _lineVisited = 0;
	
	private Value<?, ?> vlEOF = null;
	
	private boolean dataInBuffer = false;
	
	public LibreDataReader(String name, ObjectValue<XPOperand<?>> config, DMUtils dmu) {
		super(name, dmu);
		this.config = config;
	}

	@Override
	public boolean next() throws DataException {
		try {
			Boolean ores = vlEOF.asBoolean();
			if(ores == null) throw new DataException("Invalid eof value (null)");
			
			if(ores.booleanValue()) return dataInBuffer = false;
			
			++_lineVisited;
			
			return dataInBuffer = true;
		} catch (ManagedException e) {
			throw new DataException(e);
		}
		
	}

	@Override
	public String getString(String fieldName) throws DataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String fieldName) throws DataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getDouble(String fieldName) throws DataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean open() throws DataException {
		try {
			//String drVariableName = DataManFactory.getDRVariableName(name);
			//evaluator.getCurrentVariableContext().addVariable(drVariableName, DataReader.class, this);
			vlEOF = config.getRequiredAttribut("eof");
			if(vlEOF instanceof XALCalculabeValue) {
				XALCalculabeValue<?> cl = (XALCalculabeValue<?>) vlEOF;
				cl.setEvaluator(dmu.getEvaluator());
			}
			
			dmu.executeBeforeConnectionActions();
	
			
			return true;
		} catch (ManagedException e) {
			throw new DataException(e);
		}
		
	}

	@Override
	public void close() throws DataException {
		dmu.clean();
		vlEOF = null;
		
	}

	@Override
	public boolean isOpen() {
		return vlEOF != null;
	}

	@Override
	public LibreDataReader cloneDM() throws DataException {
		
		return new LibreDataReader(name, config, dmu);
	}

	@Override
	public int lineVisited() {
		return _lineVisited;
	}

	@Override
	public Integer getInteger(String fieldName) throws DataException {
		return null;
	}

	@Override
	public boolean dataInBuffer() {
		return dataInBuffer;
	}

}
