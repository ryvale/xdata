package com.exa.data.config.utils;

import java.util.HashMap;
import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.XADataSource;
import com.exa.expression.XPOperand;
import com.exa.expression.parsing.Parser.UnknownIdentifierValidation;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public class DMutils {
	private Map<String, DataReader<?>> readers = new HashMap<>();
	
	private ObjectValue<XPOperand<?>> ovRoot;
	
	public void register(String name, DataReader<?> dr) {
		readers.put(name, dr);
	}
	
	public DMutils(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, UnknownIdentifierValidation uiv, ObjectValue<XPOperand<?>> ovRoot) {
		super();
		this.ovRoot = ovRoot;
	}

	public DataReader<?> getReader(String name) { return readers.get(name); }

	public Map<String, DataReader<?>> getReaders() { return readers; }
	
}
