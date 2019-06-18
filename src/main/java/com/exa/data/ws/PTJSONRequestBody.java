package com.exa.data.ws;

import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Request.Builder;

class PTJSONRequestBody implements ParamTranslartor {

	@Override
	public RequestBody  translate(Builder rb, ArrayValue<XPOperand<?>> avParams) throws ManagedException {
		List<Value<?, XPOperand<?>>> lst = avParams.getValue();
		StringBuilder sb = new StringBuilder();
		
		for(Value<?, XPOperand<?>> vl : lst) {
			ObjectValue<XPOperand<?>> ov = vl.asRequiredObjectValue();
			
			Value<?, XPOperand<?>> vlValue = ov.getRequiredAttribut("value");
			String typeName = vlValue.typeName();
			String jsonValue;
			if("string".equals(typeName)) {
				String str = vlValue.asString();
				jsonValue = str == null ? "null" : str.replaceAll(Pattern.quote("\""), "\\\"");
			}
			else {
				Object ob = vlValue.getValue();
				if(ob == null) jsonValue = "null";
				else {
					if("date".equals(typeName) || "datetime".equals(typeName) || "time".equals(typeName)) {
						
						Date dt = (Date)ob;
						
						jsonValue = WSDataReader.DF_ISO8061.format(dt);
					}
					else jsonValue = ob.toString().replaceAll(Pattern.quote("\""), "\\\"");
				}
			}
			
			
			sb.append(", \"" + ov.getRequiredAttributAsString("name") + "\" : " + jsonValue + "\"");
		}
		
		
		MediaType mediaType = MediaType.parse("application/json;");
		
		RequestBody body = RequestBody.create(mediaType, "{" + (sb.length() > 0 ? sb.substring(2) : "") + "}");
		
		return body;
	}
	
}