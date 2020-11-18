package com.exa.data.web;

import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;

import com.exa.data.DataReader;
import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Request.Builder;

class PTJSONRequestBody implements ParamTranslartor {

	@Override
	public RequestBody  translate(Builder rb, ObjectValue<XPOperand<?>> ovParams, DataReader<?> reader) throws ManagedException {
		
		StringBuilder sb = new StringBuilder();
		
		Map<String, Value<?, XPOperand<?>>> mp = ovParams.getValue();
		
		
		for(String property : mp.keySet()) {
			Value<?, XPOperand<?>> vl = mp.get(property);
			
			String paramName,  jsonValue;
			
			Value<?, XPOperand<?>> vlValue;
			ObjectValue<XPOperand<?>> ov = vl.asObjectValue();
			if(ov == null) {
				paramName = property;
				
				vlValue = vl;
			}
			else {
				if(ov.containsAttribut("name")) paramName = ov.getRequiredAttributAsString("name");
				else paramName = property;
				
				vlValue = ov.getRequiredAttribut("exp");
			}
			
			String typeName = vlValue.typeName();
			if("string".equals(typeName)) {
				String str = vlValue.asString();
				jsonValue = str == null ? "null" :"\"" + str.replaceAll(Pattern.quote("\""), "\\\"") + "\"";
			}
			else {
				Object ob = vlValue.getValue();
				if(ob == null) jsonValue = "null";
				else {
					if("date".equals(typeName) || "datetime".equals(typeName) || "time".equals(typeName)) {
						
						Date dt = (Date)ob;
						
						jsonValue = "\"" + WSDataReader.DF_ISO8061.format(dt) + "\"";
					}
					else jsonValue = ob.toString().replaceAll(Pattern.quote("\""), "\\\"");
				}
			}
			
			sb.append(", \"" + paramName + "\" : " + jsonValue);
		}
		
		
		MediaType mediaType = MediaType.parse("application/json;");
		String strBody = "{" + (sb.length() > 0 ? sb.substring(2) : "") + "}";
		
		//System.out.println(strBody);
		
		RequestBody body = RequestBody.create(mediaType, strBody);
		
		return body;
	}
	
}