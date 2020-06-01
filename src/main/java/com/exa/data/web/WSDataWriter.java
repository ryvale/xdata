package com.exa.data.web;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.exa.data.DataException;
import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.DynamicField;
import com.exa.data.StandardDataWriterBase;

import com.exa.data.config.utils.DMUtils;
import com.exa.expression.XPOperand;
import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.IntegerValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

public class WSDataWriter extends StandardDataWriterBase<DynamicField> {
	public static  boolean debugOn = false;
	
	static final SimpleDateFormat DF_ISO8061 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX") ;
	protected final static Set<String> expTypes = new HashSet<>();
	
	protected final static Map<String, ParamTranslartor> paramsTranslators = new HashMap<>();
	
	protected final static Map<String, RMFactory> respManagers = new HashMap<>();
	
	static {
		expTypes.add("default");expTypes.add("ws");expTypes.add("value");
		
		paramsTranslators.put("requestbody-json", new PTWriterJSONRequestBody());
		
		respManagers.put("json-object", (fields, path) -> new RMJSONObject(fields, path) );
	}
	
	private WSDataSource wsDataSource;
	
	private ObjectValue<XPOperand<?>> config;
	
	private Value<?, XPOperand<?>> vlRequestType = null;
	private Value<?, XPOperand<?>> vlResponseType;
	
	private Value<?, XPOperand<?>> vlTable;
	
	private Value<?, XPOperand<?>> vlPath;
	
	private ArrayValue<XPOperand<?>> avHeaders;
	
	private Value<?, XPOperand<?>> vlParamsType;
	private ObjectValue<XPOperand<?>> ovParams;
	
	private Value<?, XPOperand<?>> vlConnectTimeout;
	private Value<?, XPOperand<?>> vlReadTimeout;
	private Value<?, XPOperand<?>> vlWriteTimeout;
	
	private ResponseManager responseMan = null;

	public WSDataWriter(String name, ObjectValue<XPOperand<?>> config, WSDataSource wsDataSource, DataReader<?> drSource, DMUtils dmu) {
		super(name, drSource, dmu);
		
		this.config = config;
		this.wsDataSource = wsDataSource;
	}

	@Override
	public int update(DataReader<?> dr) throws DataException {
		return responseMan.next() ? 1  : 0;
	}

	@Override
	public boolean open() throws DataException {
		try {
			vlTable = config.getAttribut("table");
			if(vlTable == null) vlTable = new StringValue<>("");
			else if(!"string".equals(vlTable.typeName())) throw new DataException(String.format("The property '%s' of the entity %s should be a string", "table", name));
			
			vlRequestType = config.getAttribut("requestType");
			if(vlRequestType == null) vlRequestType = new StringValue<>("post");
			else if(!"string".equals(vlRequestType.typeName())) throw new DataException(String.format("The property '%s' of the entity %s should be a string", "requestType", name));
			
			vlResponseType = config.getAttribut("responseType");
			if(vlResponseType == null) vlResponseType = new StringValue<>("json-object");
			else if(!"string".equals(vlResponseType.typeName())) throw new DataException(String.format("The property '%s' of the entity %s should be a string", "responseType", name));
			
			vlConnectTimeout = config.getAttribut("connectTimeout");
			if(vlConnectTimeout == null) vlConnectTimeout = new IntegerValue<>(15);
			else if(!"int".equals(vlConnectTimeout.typeName())) throw new DataException(String.format("The property '%s' of the entity %s should be an int", "connectTimeout", name));
			
			vlReadTimeout = config.getAttribut("readTimeout");
			if(vlReadTimeout == null) vlReadTimeout = new IntegerValue<>(30);
			else if(!"int".equals(vlReadTimeout.typeName())) throw new DataException(String.format("The property '%s' of the entity %s should be an int", "readTimeout", name));
			
			vlWriteTimeout = config.getAttribut("writeTimeout");
			if(vlWriteTimeout == null) vlWriteTimeout = new IntegerValue<>(30);
			else if(!"int".equals(vlWriteTimeout.typeName())) throw new DataException(String.format("The property '%s' of the entity %s should be an int", "writeTimeout", name));
			
			if(config.containsAttribut("headers")) {
				avHeaders =  config.getAttributAsArrayValue("headers");
				if(avHeaders == null) throw new DataException(String.format("The property '%s' of the entity %s should be an array", "headers", name));
			}
			else avHeaders = null;
			
			if(config.containsAttribut("params")) {
				ObjectValue<XPOperand<?>> ovParamsRoot = config.getAttributAsObjectValue("params");
				if(ovParamsRoot == null) throw new DataException(String.format("The property '%s' of the entity %s should be an object", "params", name));
				
				vlParamsType  = ovParamsRoot.getAttribut("type");
				
				ovParams = ovParamsRoot.getAttributAsObjectValue("items");
				
				if(ovParams == null) {
					ovParams = new ObjectValue<>();
					
					Value<?, XPOperand<?>> vl = ovParamsRoot.getAttribut("itemsAsSourceFieldsExcept");
					
					Set<String> exceptions =  new HashSet<>();
					
					
					if(vl != null) {
						
						ArrayValue<XPOperand<?>> avl = vl.asArrayValue();
						if(avl == null)
							throw new DataException(String.format("The property 'itemsAsSourceFields' of the entity %s should be an ary of string", name));
						
						for(Value<?, XPOperand<?>> ivl : vl.asArrayValue().getValue()) {
							exceptions.add(ivl.asString());
						}
					}
					drSource.executeFieldsAction(
						f -> {
							if(exceptions.contains(f.getName())) return;
							ovParams.setAttribut(f.getName(), new BooleanValue<>(Boolean.TRUE));
						}
					);
					
				}
			}
			
			ObjectValue<XPOperand<?>> ovFieldMan = config.getRequiredAttributAsObjectValue("fields");
			
			vlPath = ovFieldMan.getAttribut("path");
			if(vlPath != null && !"string".equals(vlPath.typeName())) throw new DataException(String.format("The property '%s' of the entity %s should be an object", "fields.path", name));
			
			Value<?, XPOperand<?>> vlFields = ovFieldMan.getAttribut("items");
			if(vlFields == null) throw new DataException(String.format("The property '%s' of the entity %s should be an array", "fields.items", name));
			
			ObjectValue<XPOperand<?>> ovFields = vlFields.asObjectValue();
			if(ovFields == null) {}
			else {
				Map<String, Value<?,XPOperand<?>>> mpFields = ovFields.getValue();
				
				for(String fname : mpFields.keySet()) {
					Value<?, XPOperand<?>> vlField = mpFields.get(fname);
					
					Value<?, XPOperand<?>> vlExp, vlCondition;
					String type, expType;
					BooleanValue<?> blField = vlField.asBooleanValue();
					
					if(blField == null) {
						StringValue<XPOperand<?>> sv = vlField.asStringValue();
						if(sv == null) {
							ObjectValue<XPOperand<?>> ov = vlField.asRequiredObjectValue();
							vlExp = ov.getRequiredAttribut("exp");
							type = ov.getAttributAsString("type", "string");
							expType = ov.getAttributAsString("expType", "default");
							
							vlCondition = ov.getAttribut("condition");
							if(vlCondition == null) vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
							else {
								CalculableValue<?, XPOperand<?>> clCondition = vlCondition.asCalculableValue();
								if(clCondition == null) {
									if(vlCondition.asBooleanValue() == null) throw new ManagedException(String.format("Boolean expression expected as value of 'condition' propertu for the entity %s", name));
								}
								else {
									if(!"boolean".equals(clCondition.typeName())) throw new ManagedException(String.format("Boolean expression expected as value of 'condition' propertu for the entity %s", name));
								}
							}
						}
						else {
							vlExp = sv;
							expType = "default";
							type = "string";
							vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
						}
					}
					else {
						vlExp = new StringValue<>(fname);
						expType = "default";
						type = "string";
						vlCondition = new BooleanValue<XPOperand<?>>(Boolean.TRUE);
					}
					if(!expTypes.contains(expType)) throw new DataException(String.format("Invalid expresssion type '%' for field '%s'", expType, fname));
					
					if("value".equals(expType) && !"string".equals(type)) throw new DataException(String.format("For the expression type 'value' the field type should be instead of %s for field %s", type, fname));
					
					if("default".equals(expType)) expType = "ws";
					
					DynamicField field = new DynamicField(fname, type, expType);
					field.setVlExp(vlExp);
					field.setVlCondition(vlCondition);
					
					fields.put(fname, field);
				}
				
			}
			
			
			String rt = vlResponseType.asRequiredString();
			RMFactory rf = respManagers.get(rt);
			if(rf == null) throw new DataException(String.format("The response manager %s is unknown in entity", rt, name));
			
			Request.Builder rb = wsDataSource.getRequestBuilder(vlTable.asString());
			
			if(vlParamsType != null) {
				String paramType = vlParamsType.asRequiredString();
				ParamTranslartor pt = paramsTranslators.get(paramType);
				
				if(pt == null) throw new DataException(String.format("The params type %s is unknown in entity %s", paramType, name));
				
				RequestBody body = pt.translate(rb, ovParams, drSource);
				
				if(body != null) rb.post(body);
			}
			
			dmu.executeBeforeConnectionActions();
			
			responseMan = rf.create(fields, vlPath == null ? null : vlPath.asString());
			
			responseMan.manage(rb);
			
			Request request = rb.build();
			
			OkHttpClient httpClient = new OkHttpClient();
			httpClient.setConnectTimeout(vlConnectTimeout.asInteger(), TimeUnit.SECONDS);
			httpClient.setWriteTimeout(vlWriteTimeout.asInteger(), TimeUnit.SECONDS);
			httpClient.setReadTimeout(vlReadTimeout.asInteger(), TimeUnit.SECONDS);
			
			if(debugOn)  {
				System.out.println("url :" + request.urlString());
				System.out.println("headers :" + request.headers());
			}
			
			Response response = httpClient.newCall(request).execute();
		
			
			responseMan.manage(response);
			
		} catch (ManagedException | IOException e) {
			
			if(debugOn) 
				System.out.println(e.getMessage());
			
			if(e instanceof DataException) throw (DataException)e;
			throw new DataException(e);
		}
		/*finally {
			if(response != null) try { response.} catch (Exception e2) { e2.printStackTrace(); }
		}*/
		return true;
	}

	@Override
	public void close() throws DataException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DataWriter<DynamicField> cloneDM() throws DataException {
		
		return null;
	}

	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return false;
	}

}
