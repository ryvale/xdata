package com.exa.data.sql;


public abstract class DataFormatter<T> {
	public abstract String toSQL(T rawValue, String defaultValue);
	
	@SuppressWarnings("unchecked")
	public String toSQLFormObject(Object rawValue, String defaultValue) {
		return toSQL((T)rawValue, defaultValue);
	}
	
	public String toSQLFormObject(Object rawValue) {
		return toSQLFormObject(rawValue, "null");
	}
	
	public abstract String toSQLFromString(String str, String format);
	
	public String toSQL(T rawValue) {
		return toSQL(rawValue, "null");
	}
}
