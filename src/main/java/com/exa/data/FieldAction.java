package com.exa.data;

public interface FieldAction<_FIELD extends Field> {
	void execute(_FIELD field) throws DataException;
}
