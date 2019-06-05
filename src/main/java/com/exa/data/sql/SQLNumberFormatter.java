package com.exa.data.sql;

public class SQLNumberFormatter extends DataFormatter<Number> {

	@Override
	public String toSQL(Number rawValue, String defaultValue) {
		if(rawValue == null) return defaultValue;
		
		return rawValue.toString();
	}

}
