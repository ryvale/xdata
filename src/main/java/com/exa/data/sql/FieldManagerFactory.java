package com.exa.data.sql;

import com.exa.data.DataException;
import com.exa.expression.XPOperand;
import com.exa.utils.values.ObjectValue;

class FieldManagerFactory {
	static FieldManager DEFAULT_FIELD_MANAGER = new FieldManager();
	
	FieldManager create(ObjectValue<XPOperand<?>> fieldManagerConfig) throws DataException {
		return DEFAULT_FIELD_MANAGER;
	}
}