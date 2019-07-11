package com.exa.data.config.utils;

import java.util.HashMap;
import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.XADataSource;
import com.exa.data.config.DMFGeneral;
import com.exa.expression.XPOperand;
import com.exa.expression.parsing.Parser.UnknownIdentifierValidation;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public class DMutils {
	private Map<String, DataReader<?>> readers = new HashMap<>();
	
	private ObjectValue<XPOperand<?>> ovRoot;
	
	private DMFGeneral dmf;
	
	public void register(String name, DataReader<?> dr) {
		readers.put(name, dr);
	}
	
	public DMutils(DMFGeneral dmf, ObjectValue<XPOperand<?>> ovRoot) {
		super();
		this.ovRoot = ovRoot;
		this.dmf = dmf;
		
	}

	public DataReader<?> getReader(String name) { return readers.get(name); }

	public Map<String, DataReader<?>> getReaders() { return readers; }
	
	
	
}
