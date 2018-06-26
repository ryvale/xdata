package com.exa.data;

public class Field {
	protected String name;
	protected String type;
	protected String exp = null;
	
	public Field(String name) {
		super();
		this.name = name;
	}

	public Field(String name, String type) {
		super();
		this.name = name;
		this.type = type;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public String getExp() {
		return exp;
	}

	public void setExp(String exp) {
		this.exp = exp;
	}
	
	
}
