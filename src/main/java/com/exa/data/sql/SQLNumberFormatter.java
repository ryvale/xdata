package com.exa.data.sql;

public class SQLNumberFormatter extends DataFormatter<Number> {

	@Override
	public String toSQL(Number rawValue, String defaultValue) {
		if(rawValue == null) return defaultValue;
		
		return rawValue.toString();
	}

	@Override
	public String toSQLFromString(String str, String format) {
		if(str == null || "".equals(str.trim())) return "null";
		return format == null ? str.replaceAll("[,]", ".") : str;
	}

}
