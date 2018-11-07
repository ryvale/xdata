package com.exa.data.config;

import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.expression.RMEvaluatorSetup;
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
			if("ovRoot".equals(id)) return "ObjectValue";
			String p[] = context.split("[.]");
			if(p.length<3 || !getDRVariableName(p[2]).equals(id)) return null;
			
			return "DataReader";
		});
	}
	
	public DataReader<?> getDataReader(String drName, RMEvaluatorSetup evSetup) throws ManagedException {
		
		String parts[] = drName.split("[#]");
		
		String drConfigFileName = filesRepos.getName(parts[0] +".ds.xal");
		
		ObjectValue<XPOperand<?>> rootOV = parser.parseFile(drConfigFileName, evSetup, uiv);
		
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
		
		XPEvaluator eval = new XPEvaluator();
		eval.addVariable("ovRoot", ObjectValue.class, rootOV);
		evSetup.setup(eval);
		
		eval.pushVariableContext(new MapVariableContext());
		
		return getDataReader(ovEntities, name, eval, Computing.getDefaultObjectLib(rootOV));
	}
	
	public static String getDRVariableName(String entityName) {
		return "dr"+entityName.substring(0, 1).toUpperCase()+entityName.substring(1);
	}
	
	public DataReader<?> getDataReader(ObjectValue<XPOperand<?>> ovEntities, String name, XPEvaluator eval, Map<String, ObjectValue<XPOperand<?>>> libOV) throws ManagedException {
		
		ObjectValue<XPOperand<?>> ovEntity = parser.object(ovEntities, name, eval, libOV);
		
		DataReader<?> res = getDataReader(name, ovEntity, eval);
		
		return res;
	}
	
	
	public abstract DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval) throws ManagedException;
}
