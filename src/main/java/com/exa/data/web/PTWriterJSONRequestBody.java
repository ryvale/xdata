package com.exa.data.web;

import java.util.Map;
import java.util.regex.Pattern;

import com.exa.data.DataException;
import com.exa.data.DataReader;
import com.exa.data.Field;
import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;
import com.squareup.okhttp.Request.Builder;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.RequestBody;

public class PTWriterJSONRequestBody implements ParamTranslartor {

	@Override
	public RequestBody translate(Builder rb, ObjectValue<XPOperand<?>> ovParams, DataReader<?> reader) throws ManagedException {
		StringBuilder sb = new StringBuilder();
		
		Map<String, Value<?, XPOperand<?>>> mp = ovParams.getValue();
		
		
		for(String property : mp.keySet()) {
			Value<?, XPOperand<?>> vl = mp.get(property);
			
			String paramName, expType, type, readerField;
			
			StringValue<?> sv = vl.asStringValue();
			if(sv == null) {
				CalculableValue<?, XPOperand<?>> cl = vl.asCalculableValue();
				if(cl == null) {
					ObjectValue<XPOperand<?>> ov = vl.asObjectValue();
					
					if(ov == null) {
						if(vl.asBoolean() == null)
							throw new DataException(String.format("The http param '%s' is not correctly set", property));
						paramName = property;
						expType = "reader";
						readerField = property;
						
						Field f = reader.getField(readerField);
						if(f == null) throw new DataException(String.format("The field '%s' is not found in the reader", readerField));
						
						type = f.getType();
					}
					else {
						paramName = ov.getAttributAsString("name", property);
						expType = ov.getAttributAsString("expType");
						
						if(expType == null || "reader".equals(expType)) {
							readerField = ov.getAttributAsString("exp", property);
							
							Field f = reader.getField(readerField);
							if(f == null) throw new DataException(String.format("The field '%s' is not found in the reader", readerField));
							type = reader.getField(readerField).getType();
						}
						else if("value".equals(expType)) {
							vl = ov.getRequiredAttribut("exp");
							type = vl.typeName();
							readerField = null;
						}
						else {
							type="string";
							readerField = null;
						}
					}
					
				}
				else {
					paramName = property;
					expType = "value";
					readerField = null;
					type = cl.typeName();
				}
			}
			else {
				paramName = property;
				expType = "reader";
				readerField = sv.getValue();
				
				Field f = reader.getField(readerField);
				if(f == null) throw new DataException(String.format("The field '%s' is not found in the reader", readerField));
				
				type = f.getType();
			}
			
			Object v = "value".equals(expType) ? vl.getValue() : reader.getObject(readerField);


			if(v == null) { sb.append(", \"" + paramName + "\" : null"); continue; }
			
			if("string".equals(type)) {
				String str = v.toString();
				
				sb.append(", \"" + paramName + "\" : " + (str == null ? "null" :"\"" + str.replaceAll(Pattern.quote("\""), "\\\"") + "\""));
				continue;
			}
			
			if("int".equals(type) || "boolean".equals(type)  || "float".equals(type)  || "decimal".equals(type)  || "double".equals(type)) {
				sb.append(", \"" + paramName + "\" : " + v);
				continue;
			}
			
			if("date".equals(type) || "datetime".equals(type) || "time".equals(type)) {
				String str = WSDataReader.DF_STD.format(vl.getValue());
				sb.append(", \"" + paramName + "\" : " + (str == null ? "null" :"\"" + str.replaceAll(Pattern.quote("\""), "\\\"") + "\""));
				continue;
			}
			
			String str = v.toString();
			
			sb.append(", \"" + paramName + "\" : " + (str == null ? "null" :"\"" + str.replaceAll(Pattern.quote("\""), "\\\"") + "\""));
			
		}
		
		
		MediaType mediaType = MediaType.parse("application/json; charset=UTF-8");
		String strBody = "{" + (sb.length() > 0 ? sb.substring(2) : "") + "}";
		
		if(WSDataWriter.debugOn || WSDataReader.debugOn) System.out.println(strBody);
		
		RequestBody body = RequestBody.create(mediaType, strBody);
		
		return body;
	}

}
