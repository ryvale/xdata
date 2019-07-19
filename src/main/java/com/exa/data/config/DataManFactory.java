package com.exa.data.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.XADataSource;
import com.exa.data.action.Action;
import com.exa.data.config.utils.DMUtils;
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
	public static interface DMUSetup {
		void setup(DMUtils dmu);
	}
	public static final String DMFN_SQL = "sql";
	
	public static final String DMFN_SMART = "smart";
	
	public static final String DMFN_ROW_TO_FIELD = "row-to-field";
	
	public static final String DMFN_LIBRE = "libre";
	
	public static final String DMFN_XLITERAL = "x-literal";
	
	public static final String DMFN_WS = "ws";
	
	public static final String DMFN_MAP = "map";
	
	public static final String DMFN_SP_SQL = "sp-sql";
	
	public final static XALParser parser = new XALParser();
	
	protected FilesRepositories filesRepos;
	
	protected UnknownIdentifierValidation uiv;
	
	protected Map<String, XADataSource> dataSources = new HashMap<>();
	
	protected String defaultDataSource;
	
	protected DMFGeneral dmuDmf;
	
	protected DMUSetup dmuSetup;

	public DataManFactory(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMUSetup dmuSetup, UnknownIdentifierValidation uiv) {
		this.filesRepos=filesRepos;
		
		this.dataSources = dataSources;
		this.defaultDataSource = defaultDataSource;
		
		this.dmuSetup = dmuSetup;
		
		this.uiv = uiv;
		
	}
	
	public DataManFactory(DMFGeneral dmuDmf) {
		this(dmuDmf.getFilesRepos(), dmuDmf.getDataSources(), dmuDmf.getDefaultDataSource(), dmuDmf.getDmuSetup(), dmuDmf.getUiv());
		this.dmuDmf = dmuDmf;
	}
	
	public void initialize() { 
		if(dmuDmf == null) {
			dmuDmf = new DMFGeneral(DMFN_SMART, filesRepos, dataSources, defaultDataSource, dmuSetup);
			dmuDmf.initialize();
		}
	}
	
	public DataManFactory(FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMUSetup dmuSetup) {
		this(filesRepos, dataSources, defaultDataSource, dmuSetup, (id, context) -> {
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
		
		DMUtils dmu = new DMUtils(dmuDmf, ovRoot, evaluator, vc, dmuSetup);
		dmuSetup.setup(dmu);
				
		evaluator.addVariable("rootOv", ObjectValue.class, ovRoot);
		
		evaluator.pushVariableContext(vc);
		
		DataReader<?> dr = getDataReader(ovEntities, name, Computing.getDefaultObjectLib(ovRoot), dmu);
		
		vc.addVariable("rootDr", DataReader.class, dr);
		
		vc.addVariable("dmu", DMUtils.class, dmu);
		
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
		DMUtils dmu = new DMUtils(dmuDmf/*, parser*/, ovRoot, evaluator, vc, dmuSetup);
		dmuSetup.setup(dmu);
		
		evaluator.addVariable("rootOv", ObjectValue.class, ovRoot);
	
		evaluator.pushVariableContext(vc);
		
		DataWriter<?> dm = getDataWriter(ovEntities, name, evaluator, vc, drSource, Computing.getDefaultObjectLib(ovRoot), dmu, preventInsertion, preventUpdate);
		
		vc.addVariable("sourceDr", DataReader.class, drSource);
		
		vc.addVariable("rootDw", DataWriter.class, dm);
		
		vc.addVariable("dmu", DMUtils.class, dmu);
		
		return dm;
	}
	
	public DataWriter<?> getDataWriter(String drName, DCEvaluatorSetup evSetup, DataReader<?> drSource) throws ManagedException {
		return getDataWriter(drName, evSetup, drSource, true, true);
	}
	
	public static String getDRVariableName(String entityName) {
		return entityName.substring(0, 1).toUpperCase()+entityName.substring(1)+"Dr";
	}
	
	public DataReader<?> getDataReader(ObjectValue<XPOperand<?>> ovEntities, String name, Map<String, ObjectValue<XPOperand<?>>> libOV, DMUtils dmu) throws ManagedException {
		
		ObjectValue<XPOperand<?>> ovEntity = parser.object(ovEntities, name, dmu.getEvaluator(), dmu.getVc(), libOV);
		
		ObjectValue<XPOperand<?>> ovBeforeConnectionActions = ovEntities.getPathAttributAsObjecValue(name + ".beforeConnection");
		if(ovBeforeConnectionActions != null) {
			Map<String, Value<?, XPOperand<?>>> mpBCA = ovBeforeConnectionActions.getValue();
			
			for(String bcaName: mpBCA.keySet()) {
				Action ac  = dmu.registerBeforeAction(bcaName, mpBCA.get(bcaName));
				if(ac == null) throw new ManagedException(String.format("the action %s in 'beforeConnection' for entity '%s' seem to be invalid", bcaName, name));
			}
		}
		
		DataReader<?> res = getDataReader(name, ovEntity, dmu);
		
		return res;
	}
	
	public DataWriter<?> getDataWriter(ObjectValue<XPOperand<?>> ovEntities, String name, XPEvaluator eval, VariableContext vc, DataReader<?> drSource, Map<String, ObjectValue<XPOperand<?>>> libOV, DMUtils dmu, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		
		ObjectValue<XPOperand<?>> ovEntity = parser.object(ovEntities, name, eval, vc, libOV);
		
		ObjectValue<XPOperand<?>> ovBeforeConnectionActions = ovEntities.getPathAttributAsObjecValue(name + ".beforeConnection");
		if(ovBeforeConnectionActions != null) {
			Map<String, Value<?, XPOperand<?>>> mpBCA = ovBeforeConnectionActions.getValue();
			
			for(String bcaName: mpBCA.keySet()) {
				Action ac  = dmu.registerBeforeAction(bcaName, mpBCA.get(bcaName));
				if(ac == null) throw new ManagedException(String.format("the action %s in 'beforeConnection' for entity '%s' seem to be invalid", bcaName, name));
			}
		}
		
		DataWriter<?> res = getDataWriter(name, ovEntity, drSource, dmu, preventInsertion, preventUpdate);
		
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
	
	public DMUSetup getDmuSetup() {
		return dmuSetup;
	}

	public void setDmuSetup(DMUSetup dmuSetup) {
		this.dmuSetup = dmuSetup;
	}

	public abstract DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, DMUtils dmu) throws ManagedException;
	
	public abstract DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity, DataReader<?> drSource, DMUtils dmu, boolean preventInsertion, boolean preventUpdate) throws ManagedException;
}
