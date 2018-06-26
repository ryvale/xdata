package com.exa.data.sql;

public class XAFieldManager extends FieldManager {
	private String prefix;
	
	public XAFieldManager(String prefix) {
		super();
		this.prefix = prefix;
	}

	@Override
	public String toSQL(String name) {
		return prefix + name;
	}

	
}
