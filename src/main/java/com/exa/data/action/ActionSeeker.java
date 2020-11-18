package com.exa.data.action;

import com.exa.expression.XPOperand;
import com.exa.utils.values.Value;

public interface ActionSeeker {
	Action found(String name, Value<?, XPOperand<?>> actionConfig);
}
