package com.exa.data.config;

import java.io.IOException;
import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.expression.DCEvaluatorSetup;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.MapVariableContext;
import com.exa.expression.eval.XPEvaluator;
import com.exa.expression.parsing.Parser.UnknownIdentifierValidation;
import com.exa.lang.parsing.Computing;
import com.exa.lang.parsing.XALParser;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public abstract class DataManFactory {
	protected final static XALParser parser = new XALParser();
	
	protected FilesRepositories filesRepos;
	
	protected UnknownIdentifierValidation uiv;

	public DataManFactory(FilesRepositories filesRepos, UnknownIdentifierValidation uiv) {
		super();
		this.filesRepos=filesRepos;
		
		this.uiv = uiv;
	}
	
	public DataManFactory(FilesRepositories filesRepos) {
		this(filesRepos, (id, context) -> {
			if("rootDr".equals(id) || "sourceDr".equals(id)) return "DataReader";
			
			if("rootDw".equals(id)) return "DataWriter";
			
			if("rootOv".equals(id)) return "ObjectValue";
			
			String p[] = context.split("[.]");
			if(p.length<3 || !"this".equals(id)) return null;
			
			return "DataReader";
		});
	}
	
	public DataReader<?> getDataReader(String drName, DCEvaluatorSetup evSetup) throws ManagedException {
		
		String parts[] = drName.split("[#]");
		
		String drConfigFileName = filesRepos.getName(parts[0] +".ds.xal");
		
		/**/
		
		Computing computing = parser.getComputeObjectFormFile(drConfigFileName, evSetup, uiv);
		
		XPEvaluator evaluator = computing.getXPEvaluator();
		
		ObjectValue<XPOperand<?>> rootOV = computing.execute();
		
		try {
			computing.closeCharReader();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		ObjectValue<XPOperand<?>> ovEntities = rootOV.getAttributAsObjectValue("entities");
		
		String name;
		if(parts.length>1) {
			name = parts[1];
		}
		else {
			Map<String, Value<?, XPOperand<?>>> mapEntities = ovEntities.getValue();
			if(mapEntities.size() == 0) throw new ManagedException(String.format("No entity found while seeking %s", drName));
			name = mapEntities.keySet().iterator().next();
		}
		
		evaluator.addVariable("rootOv", ObjectValue.class, rootOV);
		
		VariableContext vc = new MapVariableContext(evaluator.getCurrentVariableContext());
		evaluator.pushVariableContext(vc);
		
		DataReader<?> dr = getDataReader(ovEntities, name, evaluator, vc, Computing.getDefaultObjectLib(rootOV));
		
		vc.addVariable("rootDr", DataReader.class, dr);
		
		return dr;
	}
	
	public DataWriter<?> getDataWriter(String drName, DCEvaluatorSetup evSetup, DataReader<?> drSource, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		
		String parts[] = drName.split("[#]");
		
		String drConfigFileName = filesRepos.getName(parts[0] +".ds.xal");
		
		/**/
		
		Computing computing = parser.getComputeObjectFormFile(drConfigFileName, evSetup, uiv);
		
		XPEvaluator evaluator = computing.getXPEvaluator();
		
		ObjectValue<XPOperand<?>> rootOV = computing.execute();
		
		try {
			computing.closeCharReader();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		ObjectValue<XPOperand<?>> ovEntities = rootOV.getAttributAsObjectValue("entities");
		
		String name;
		if(parts.length>1) {
			name = parts[1];
		}
		else {
			Map<String, Value<?, XPOperand<?>>> mapEntities = ovEntities.getValue();
			if(mapEntities.size() == 0) throw new ManagedException(String.format("No entity found while seeking %s", drName));
			name = mapEntities.keySet().iterator().next();
		}
		
		evaluator.addVariable("rootOv", ObjectValue.class, rootOV);
		
		VariableContext vc = new MapVariableContext(evaluator.getCurrentVariableContext());
		evaluator.pushVariableContext(vc);
		
		DataWriter<?> dm = getDataWriter(ovEntities, name, evaluator, vc, drSource, Computing.getDefaultObjectLib(rootOV), preventInsertion, preventUpdate);
		
		vc.addVariable("sourceDr", DataReader.class, drSource);
		
		vc.addVariable("rootDw", DataWriter.class, dm);
		return dm;
	}
	
	public DataWriter<?> getDataWriter(String drName, DCEvaluatorSetup evSetup, DataReader<?> drSource) throws ManagedException {
		return getDataWriter(drName, evSetup, drSource, true, true);
	}
	
	public static String getDRVariableName(String entityName) {
		return entityName.substring(0, 1).toUpperCase()+entityName.substring(1)+"Dr";
	}
	
	public DataReader<?> getDataReader(ObjectValue<XPOperand<?>> ovEntities, String name, XPEvaluator eval, VariableContext vc, Map<String, ObjectValue<XPOperand<?>>> libOV) throws ManagedException {
		
		ObjectValue<XPOperand<?>> ovEntity = parser.object(ovEntities, name, eval, vc, libOV);
		
		DataReader<?> res = getDataReader(name, ovEntity, eval, vc);
		
		return res;
	}
	
	public DataWriter<?> getDataWriter(ObjectValue<XPOperand<?>> ovEntities, String name, XPEvaluator eval, VariableContext vc, DataReader<?> drSource, Map<String, ObjectValue<XPOperand<?>>> libOV, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		
		ObjectValue<XPOperand<?>> ovEntity = parser.object(ovEntities, name, eval, vc, libOV);
		
		DataWriter<?> res = getDataWriter(name, ovEntity, eval, vc, drSource, preventInsertion, preventUpdate);
		
		return res;
	}
	
	
	public abstract DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext vc) throws ManagedException;
	
	public abstract DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext vc, DataReader<?> drSource, boolean preventInsertion, boolean preventUpdate) throws ManagedException;
}
