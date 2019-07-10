package com.exa.data.config.utils;

import java.util.HashMap;
import java.util.Map;

import com.exa.data.DataReader;

public class DMutils {
	private Map<String, DataReader<?>> readers = new HashMap<>();
	
	public void register(String name, DataReader<?> dr) {
		readers.put(name, dr);
	}
	
	public DataReader<?> getReader(String name) { return readers.get(name); }

	public Map<String, DataReader<?>> getReaders() { return readers; }
	
}
