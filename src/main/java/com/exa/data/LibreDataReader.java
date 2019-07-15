package com.exa.data;

import java.util.Date;

import com.exa.data.config.utils.DMutils;
import com.exa.expression.XPOperand;

import com.exa.lang.expression.XALCalculabeValue;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public class LibreDataReader extends StandardDataReaderBase<Field> {
	private ObjectValue<XPOperand<?>> config;
	
	protected Integer _lineVisited = 0;
	
	private Value<?, ?> vlEOF = null;
	
	
	public LibreDataReader(String name, ObjectValue<XPOperand<?>> config/*, XPEvaluator evaluator, VariableContext variableContext*/, DMutils dmu) {
		super(name/*, evaluator, variableContext*/, dmu);
		this.config = config;
	}

	@Override
	public boolean next() throws DataException {
		try {
			Boolean ores = vlEOF.asBoolean();
			if(ores == null) throw new DataException("Invalid eof value (null)");
			boolean res = ores.booleanValue();
			if(res) return false;
			
			++_lineVisited;
			
			return true;
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
			
			/*for(DataReader<?> dr : dmu.getReaders().values()) {
				dr.open();
			}*/
			
			return true;
		} catch (ManagedException e) {
			throw new DataException(e);
		}
		
	}

	@Override
	public void close() throws DataException {
		/*for(DataReader<?> dr : dmu.getReaders().values()) {
			try { dr.close(); } catch (Exception e) { e.printStackTrace(); }
		}*/
		dmu.clean();
		vlEOF = null;
		
	}

	@Override
	public boolean isOpen() {
		return vlEOF != null;
	}

	@Override
	public StandardDataReaderBase<Field> cloneDM() throws DataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int lineVisited() {
		return _lineVisited;
	}

	@Override
	public Integer getInteger(String fieldName) throws DataException {
		// TODO Auto-generated method stub
		return null;
	}

}
