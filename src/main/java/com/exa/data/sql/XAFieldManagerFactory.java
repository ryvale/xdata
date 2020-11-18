package com.exa.data.sql;

import com.exa.data.DataException;
import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;

class XAFieldManagerFactory extends FieldManagerFactory {

	@Override
	FieldManager create(ObjectValue<XPOperand<?>> fieldManagerConfig) throws DataException {
		String prefix;
		if(fieldManagerConfig == null) throw new DataException(String.format("No field manager provided while expected on in xa field manager factory."));
		try {
			prefix = fieldManagerConfig.getAttributAsString("prefix");
		} catch (ManagedException e) {
			throw new DataException(e);
		}
		if(prefix == null) throw new DataException(String.format("The property prefix is not defined for XA Field Manager"));
		
		return new XAFieldManager(prefix);
	}
	
}