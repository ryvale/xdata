package com.exa.data.web;

import com.exa.data.DataReader;
import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;
import okhttp3.Request;
import okhttp3.RequestBody;


interface ParamTranslartor {
	RequestBody translate(Request.Builder rb, ObjectValue<XPOperand<?>> ovParams, DataReader<?> reader) throws ManagedException;
}