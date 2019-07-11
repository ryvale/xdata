package com.exa.data.expression.macro;

import com.exa.data.DataReader;
import com.exa.utils.ManagedException;

public class MReaderValue<T> extends Macro<T> {
	
	public static interface ReaderLogic<T> {
		T value(DataReader<?> dr, String fieldName);
		
		String typeName();
	}
	
	private ReaderLogic<T> readerLogic;
	
	protected DataReader<?> dr;
	
	protected String fieldName;

	public MReaderValue(ReaderLogic<T> readerLogic, DataReader<?> dr, String fieldName) {
		super();
		this.readerLogic = readerLogic;
		this.dr = dr;
		this.fieldName = fieldName;
	}

	@Override
	public String typeName() {
		return readerLogic.typeName();
	}

	@Override
	public T value() throws ManagedException {
		return readerLogic.value(dr, fieldName);
	}
	
}
