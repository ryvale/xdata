package com.exa.data.sql;

public class SQLStringFormatter  extends DataFormatter<String> {

	@Override
	public String toSQL(String rawValue, String defaultValue) {
		if(rawValue == null || "".equals(rawValue)) return defaultValue;
		
		return "'" + rawValue.replaceAll("'", "''") + "'";
	}

	@Override
	public String toSQLFromString(String str, String format) {
		return toSQL(str);
	}

}
