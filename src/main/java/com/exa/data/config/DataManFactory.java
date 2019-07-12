package com.exa.data.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.XADataSource;
import com.exa.data.config.utils.DMutils;
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
public static final String DMFN_SQL = "sql";
	
	public static final String DMFN_SMART = "smart";
	
	public static final String DMFN_ROW_TO_FIELD = "row-to-field";
	
	public static final String DMFN_LIBRE = "libre";
	
	public static final String DMFN_XLITERAL = "x-literal";
	
	public static final String DMFN_WS = "ws";
	
	public static final String DMFN_MAP = "map";
	
	public static final String DMFN_SP_SQL = "sp-sql";
	
	protected final static XALParser parser = new XALParser();
	
	protected FilesRepositories filesRepos;
	
	protected UnknownIdentifierValidation uiv;
	
	protected Map<String, XADataSource> dataSources = new HashMap<>();
	
	protected String defaultDataSource;
	
	protected DMFGeneral dmuDmf;

	public DataManFactory(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, UnknownIdentifierValidation uiv) {
		this.filesRepos=filesRepos;
		
		this.dataSources = dataSources;
		this.defaultDataSource = defaultDataSource;
		
		this.uiv = uiv;
	}
	
	public DataManFactory(DMFGeneral dmuDmf) {
		this(dmuDmf.getFilesRepos(), dmuDmf.getDataSources(), dmuDmf.getDefaultDataSource(), dmuDmf.getUiv());
		this.dmuDmf = dmuDmf;
	}
	
	public void initialize() { 
		if(dmuDmf == null) {
			dmuDmf = new DMFGeneral(DMFN_SMART, filesRepos, dataSources, defaultDataSource);
			dmuDmf.initialize();
		}
	}
	
	public DataManFactory(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource) {
		this(filesRepos, dataSources, defaultDataSource, (id, context) -> {
			if("rootDr".equals(id) || "sourceDr".equals(id)) return "DataReader";
			
			if("rootDw".equals(id)) return "DataWriter";
			
			if("rootOv".equals(id)) return "ObjectValue";
			
			if("dmu".equals(id)) return "DMUtils";
			
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
		
		ObjectValue<XPOperand<?>> ovRoot = computing.execute();
		
		try { computing.closeCharReader(); } catch (IOException e) { e.printStackTrace(); }
		
		ObjectValue<XPOperand<?>> ovEntities = ovRoot.getAttributAsObjectValue("entities");
		
		String name;
		if(parts.length>1) {
			name = parts[1];
		}
		else {
			Map<String, Value<?, XPOperand<?>>> mapEntities = ovEntities.getValue();
			if(mapEntities.size() == 0) throw new ManagedException(String.format("No entity found while seeking %s", drName));
			name = mapEntities.keySet().iterator().next();
		}
		VariableContext vc = new MapVariableContext(evaluator.getCurrentVariableContext());
		
		DMutils dmu = new DMutils(dmuDmf, parser, ovRoot, evaluator, vc);
		
		ObjectValue<XPOperand<?>> ovVariables = ovEntities.getPathAttributAsObjecValue(name + ".variables");
		
		
		
		if(ovVariables != null) {
			Map<String, Value<?, XPOperand<?>>> mpVariables = ovVariables.getValue();
			
			for(String varName : mpVariables.keySet()) {
				Value<?, XPOperand<?>> vlVariable = mpVariables.get(varName);
				
				ObjectValue<XPOperand<?>> ovVariable = vlVariable.asRequiredObjectValue();
				
				if("DataReader".equals(ovVariable.getRequiredAttributAsString("type"))) {
					String drVarName = ovVariable.getRequiredAttributAsString("ref");
					ObjectValue<XPOperand<?>> ovDr = ovEntities.getAttributAsObjectValue(drVarName);
					
					String dmfName = ovDr.getRequiredAttributAsString("type");
					DataManFactory dmf = dmuDmf.getDMF(dmfName);
					if(dmf == null) throw new ManagedException(String.format("Invalid data manager factory '%s'", dmfName)) ;
					
					VariableContext varVC = new MapVariableContext(vc);
					
					DMutils varDmu = new DMutils(dmuDmf, parser, ovRoot, evaluator, varVC);
					
					varVC.addVariable("dmu", DMutils.class, varDmu);
					DataReader<?> vdr = dmf.getDataReader(ovEntities, drVarName, evaluator, varVC, Computing.getDefaultObjectLib(ovRoot), varDmu);
					dmu.register(varName, vdr);
					continue;
				}
			}
		}
		
		evaluator.addVariable("rootOv", ObjectValue.class, ovRoot);
		
		evaluator.pushVariableContext(vc);
		
		DataReader<?> dr = getDataReader(ovEntities, name, evaluator, vc, Computing.getDefaultObjectLib(ovRoot), dmu);
		
		vc.addVariable("rootDr", DataReader.class, dr);
		
		vc.addVariable("dmu", DMutils.class, dmu);
		
		return dr;
	}
	
	public DataWriter<?> getDataWriter(String drName, DCEvaluatorSetup evSetup, DataReader<?> drSource, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		
		String parts[] = drName.split("[#]");
		
		String drConfigFileName = filesRepos.getName(parts[0] +".ds.xal");
		
		Computing computing = parser.getComputeObjectFormFile(drConfigFileName, evSetup, uiv);
		
		XPEvaluator evaluator = computing.getXPEvaluator();
		
		ObjectValue<XPOperand<?>> ovRoot = computing.execute();
		
		try { computing.closeCharReader();	} catch (IOException e) { e.printStackTrace();	}
		
		ObjectValue<XPOperand<?>> ovEntities = ovRoot.getAttributAsObjectValue("entities");
		
		String name;
		if(parts.length>1) {
			name = parts[1];
		}
		else {
			Map<String, Value<?, XPOperand<?>>> mapEntities = ovEntities.getValue();
			if(mapEntities.size() == 0) throw new ManagedException(String.format("No entity found while seeking %s", drName));
			name = mapEntities.keySet().iterator().next();
		}
		
		
		VariableContext vc = new MapVariableContext(evaluator.getCurrentVariableContext());
		DMutils dmu = new DMutils(dmuDmf, parser, ovRoot, evaluator, vc);
		
		ObjectValue<XPOperand<?>> ovVariables = ovEntities.getPathAttributAsObjecValue(name + ".variables");
		if(ovVariables != null) {
			Map<String, Value<?, XPOperand<?>>> mpVariables = ovVariables.getValue();
			
			for(String varName : mpVariables.keySet()) {
				Value<?, XPOperand<?>> vlVariable = mpVariables.get(varName);
				
				ObjectValue<XPOperand<?>> ovVariable = vlVariable.asRequiredObjectValue();
				
				if("DataReader".equals(ovVariable.getRequiredAttributAsString("type"))) {
					String drVarName = ovVariable.getRequiredAttributAsString("ref");
					ObjectValue<XPOperand<?>> ovDr = ovEntities.getAttributAsObjectValue(drVarName);
					
					String dmfName = ovDr.getRequiredAttributAsString("type");
					DataManFactory dmf = dmuDmf.getDMF(dmfName);
					if(dmf == null) throw new ManagedException(String.format("Invalid data manager factory '%s'", dmfName)) ;
					
					VariableContext varVC = new MapVariableContext(vc);
					
					DMutils varDmu = new DMutils(dmuDmf, parser, ovRoot, evaluator, varVC);
					
					varVC.addVariable("dmu", DMutils.class, varDmu);
					DataReader<?> vdr = dmf.getDataReader(ovEntities, drVarName, evaluator, varVC, Computing.getDefaultObjectLib(ovRoot), varDmu);
					dmu.register(varName, vdr);
					continue;
				}
			}
		}
		
		evaluator.addVariable("rootOv", ObjectValue.class, ovRoot);
		
		
		evaluator.pushVariableContext(vc);
		
		DataWriter<?> dm = getDataWriter(ovEntities, name, evaluator, vc, drSource, Computing.getDefaultObjectLib(ovRoot), dmu, preventInsertion, preventUpdate);
		
		vc.addVariable("sourceDr", DataReader.class, drSource);
		
		vc.addVariable("rootDw", DataWriter.class, dm);
		
		vc.addVariable("dmu", DMutils.class, dmu);
		
		return dm;
	}
	
	public DataWriter<?> getDataWriter(String drName, DCEvaluatorSetup evSetup, DataReader<?> drSource) throws ManagedException {
		return getDataWriter(drName, evSetup, drSource, true, true);
	}
	
	public static String getDRVariableName(String entityName) {
		return entityName.substring(0, 1).toUpperCase()+entityName.substring(1)+"Dr";
	}
	
	public DataReader<?> getDataReader(ObjectValue<XPOperand<?>> ovEntities, String name, XPEvaluator eval, VariableContext vc, Map<String, ObjectValue<XPOperand<?>>> libOV, DMutils dmu) throws ManagedException {
		
		ObjectValue<XPOperand<?>> ovEntity = parser.object(ovEntities, name, eval, vc, libOV);
		
		DataReader<?> res = getDataReader(name, ovEntity, eval, vc, dmu);
		
		return res;
	}
	
	public DataWriter<?> getDataWriter(ObjectValue<XPOperand<?>> ovEntities, String name, XPEvaluator eval, VariableContext vc, DataReader<?> drSource, Map<String, ObjectValue<XPOperand<?>>> libOV, DMutils dmu, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		
		ObjectValue<XPOperand<?>> ovEntity = parser.object(ovEntities, name, eval, vc, libOV);
		
		DataWriter<?> res = getDataWriter(name, ovEntity, eval, vc, drSource, dmu, preventInsertion, preventUpdate);
		
		return res;
	}
	
	public FilesRepositories getFilesRepos() {
		return filesRepos;
	}

	public void setFilesRepos(FilesRepositories filesRepos) {
		this.filesRepos = filesRepos;
	}

	public UnknownIdentifierValidation getUiv() {
		return uiv;
	}

	public void setUiv(UnknownIdentifierValidation uiv) {
		this.uiv = uiv;
	}

	public Map<String, XADataSource> getDataSources() {
		return dataSources;
	}

	public void setDataSources(Map<String, XADataSource> dataSources) {
		this.dataSources = dataSources;
	}

	public String getDefaultDataSource() {
		return defaultDataSource;
	}

	public void setDefaultDataSource(String defaultDataSource) {
		this.defaultDataSource = defaultDataSource;
	}

	public abstract DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext vc, DMutils dmu) throws ManagedException;
	
	public abstract DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity, XPEvaluator eval, VariableContext vc, DataReader<?> drSource, DMutils dmu, boolean preventInsertion, boolean preventUpdate) throws ManagedException;
}
