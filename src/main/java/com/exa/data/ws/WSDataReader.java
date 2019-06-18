package com.exa.data.ws;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.exa.data.DataException;
import com.exa.data.DynamicField;

import com.exa.data.StandardDataReaderBase;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
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
	static final SimpleDateFormat DF_ISO8061 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX") ;
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
	
	private ArrayValue<XPOperand<?>> avHeaders;
	
	private Value<?, XPOperand<?>> vlParamsType;
	private ArrayValue<XPOperand<?>> avParams;
	
	protected int _lineVisited = 0;
	
	private ResponseManager responseMan = null;
	
	//private ArrayValue<XPOperand<?>> avFields;
	
	public WSDataReader(String name, XPEvaluator evaluator, VariableContext variableContext, ObjectValue<XPOperand<?>> config, WSDataSource wsDataSource) {
		super(name, evaluator, variableContext);
		
		this.config = config;
		this.wsDataSource = wsDataSource;
	}
	
	/*public RestWSDataReader(String name, ObjectValue<XPOperand<?>> config, XPEvaluator evaluator,
			VariableContext variableContext, FilesRepositories filesRepos, Map<String, DataSource> dataSources,
			String defaultDataSource, RestTemplate restTemplate) {
		super(name, config, evaluator, variableContext, filesRepos, dataSources, defaultDataSource);
		
		this.config = config;
		this.restTemplate = restTemplate;
	}*/
	
	

	@Override
	public boolean next() throws DataException {
		return responseMan.next();
	}

	@Override
	public String getString(String fieldName) throws DataException {
		// TODO Auto-generated method stub
		return null;
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
				ObjectValue<XPOperand<?>> ovParams = config.getAttributAsObjectValue("params");
				if(ovParams == null) throw new DataException(String.format("The property '%s' of the entity %s should be an object", "params", name));
				
				vlParamsType  = ovParams.getAttribut("type");
				
				avParams = ovParams.getRequiredAttributAsArrayValue("items");
			}
			
			Value<?, XPOperand<?>> vlFields = config.getPathAttribut("fields.items");
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
			
			
			Request.Builder rb = wsDataSource.getRequestBuilder(vlTable.asString());
			
			RequestBody body;
			if(vlParamsType != null) {
				String paramType = vlParamsType.asRequiredString();
				ParamTranslartor pt = paramsTranslators.get(paramType);
				
				if(pt == null) throw new DataException(String.format("The params type %s is unknown in entity %s", paramType, name));
				
				body = pt.translate(rb, avParams);
				
				if(body != null) rb.post(body);
			}
			
			String rt = vlResponseType.asRequiredString();
			RMFactory rf = respManagers.get(rt);
			if(rf == null) throw new DataException(String.format("The response manager %s is unknown in entity", rt, name));
			
			ResponseManager rm = rf.create(fields, vlTable.asString());
			
			Request request = rb.build();
			
			OkHttpClient httpClient = new OkHttpClient();
			
			Response response = httpClient.newCall(request).execute();
			
			rm.open(response);
			
			rm.close();
			
		} catch (ManagedException | IOException e) {
			throw new DataException(e);
		}
		/*finally {
			if(response != null) try { response.} catch (Exception e2) { e2.printStackTrace(); }
		}*/
		return false;
	}

	@Override
	public void close() throws DataException {
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

}
