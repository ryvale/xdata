package com.exa.data.web;

import java.io.IOException;
import java.text.SimpleDateFormat;
//import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
//import java.util.List;
import java.util.Map;
import java.util.Set;

import com.exa.data.DataException;
import com.exa.data.DynamicField;

import com.exa.data.StandardDataReaderBase;
import com.exa.data.config.utils.DMUtils;

import com.exa.expression.XPOperand;

import com.exa.utils.ManagedException;
import com.exa.utils.values.ArrayValue;
import com.exa.utils.values.BooleanValue;
import com.exa.utils.values.CalculableValue;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.StringValue;
import com.exa.utils.values.Value;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

public class WSDataReader extends StandardDataReaderBase<DynamicField> {
	public static  boolean debugOn = false;
	
	static final SimpleDateFormat DF_ISO8061 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX") ;
	static final SimpleDateFormat DF_STD = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;
	protected final static Set<String> expTypes = new HashSet<>();
	
	protected final static Map<String, ParamTranslartor> paramsTranslators = new HashMap<>();
	
	protected final static Map<String, RMFactory> respManagers = new HashMap<>();
	
	static {
		expTypes.add("default");expTypes.add("ws");expTypes.add("value");
		
		paramsTranslators.put("requestbody-json", new PTJSONRequestBody());
		
		respManagers.put("json-object", (fields, path) -> new RMJSONObject(fields, path) );
	}
	
	private ObjectValue<XPOperand<?>> config;
	
	private WSDataSource wsDataSource;
	
	private Value<?, XPOperand<?>> vlRequestType = null;
	private Value<?, XPOperand<?>> vlResponseType;
	
	private Value<?, XPOperand<?>> vlTable;
	
	private Value<?, XPOperand<?>> vlPath;
	
	private ArrayValue<XPOperand<?>> avHeaders;
	
	private Value<?, XPOperand<?>> vlParamsType;
	private ObjectValue<XPOperand<?>> ovParams;
	
	protected int _lineVisited = 0;
	
	private boolean dataInBuffer = false;
	
	private ResponseManager responseMan = null;
	
	public WSDataReader(String name, ObjectValue<XPOperand<?>> config, WSDataSource wsDataSource, DMUtils dmu) {
		super(name, dmu);
		
		this.config = config;
		this.wsDataSource = wsDataSource;
	}
	
	

	@Override
	public boolean next() throws DataException {
		boolean res = dataInBuffer = responseMan.next();
		
		if(res) ++_lineVisited;
		return res;
	}

	@Override
	public String getString(String fieldName) throws DataException {
		
		try {
			return responseMan.getString(fieldName);
		} catch (ManagedException e) {
			throw new DataException(e);
		}
	}

	@Override
	public Integer getInteger(String fieldName) throws DataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String fieldName) throws DataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double getDouble(String fieldName) throws DataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean open() throws DataException {
		try {
			vlTable = config.getAttribut("table");
			if(vlTable == null) vlTable = new StringValue<>("");
			else if(!"string".equals(vlTable.typeName())) throw new DataException(String.format("The property '%s' of the entity %s should be a string", "table", name));
			
			vlRequestType = config.getRequiredAttribut("requestType");
			if(!"string".equals(vlRequestType.typeName())) throw new DataException(String.format("The property '%s' of the entity %s should be a string", "requestType", name));
			
			vlResponseType = config.getRequiredAttribut("responseType");
			if(!"string".equals(vlResponseType.typeName())) throw new DataException(String.format("The property '%s' of the entity %s should be a string", "responseType", name));
			
			if(config.containsAttribut("headers")) {
				avHeaders =  config.getAttributAsArrayValue("headers");
				if(avHeaders == null) throw new DataException(String.format("The property '%s' of the entity %s should be an array", "headers", name));
			}
			else avHeaders = null;
			
			if(config.containsAttribut("params")) {
				ObjectValue<XPOperand<?>> ovParamsRoot = config.getAttributAsObjectValue("params");
				if(ovParamsRoot == null) throw new DataException(String.format("The property '%s' of the entity %s should be an object", "params", name));
				
				vlParamsType  = ovParamsRoot.getAttribut("type");
				
				ovParams = ovParamsRoot.getRequiredAttributAsObjectValue("items");
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
				
				RequestBody body = pt.translate(rb, ovParams, null);
				
				if(body != null) rb.post(body);
			}
			

			dmu.executeBeforeConnectionActions();
			
			responseMan = rf.create(fields, vlPath == null ? null : vlPath.asString());
			
			responseMan.manage(rb);
			
			Request request = rb.build();
			
			OkHttpClient httpClient = new OkHttpClient();
			
			Response response = httpClient.newCall(request).execute();
			
			responseMan.manage(response);
			
		} catch (ManagedException | IOException e) {
			throw new DataException(e);
		}
		
		return responseMan.isOpen();
	}

	@Override
	public void close() throws DataException {
		/*for(DataReader<?> dr : dmu.getReaders().values()) {
			try { dr.close(); } catch(DataException e) { e.printStackTrace();}
		}*/
		
		dmu.clean();
		responseMan = null;
		
	}

	@Override
	public int lineVisited() {
		return _lineVisited;
	}

	@Override
	public boolean isOpen() {
		return responseMan != null;
	}

	@Override
	public WSDataReader cloneDM() throws DataException {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public boolean dataInBuffer() {
		return dataInBuffer;
	}

}
