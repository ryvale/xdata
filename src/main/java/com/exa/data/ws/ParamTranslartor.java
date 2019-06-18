package com.exa.data.ws;

import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;

interface ParamTranslartor {
	RequestBody translate(Request.Builder rb, ObjectValue<XPOperand<?>> ovParams) throws ManagedException;
}