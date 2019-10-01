package com.exa.data.config;

import java.util.HashMap;
import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.DataWriter;
import com.exa.data.XADataSource;
import com.exa.data.config.utils.DMUtils;
import com.exa.data.expression.DCEvaluatorSetup;
import com.exa.expression.XPOperand;

import com.exa.expression.parsing.Parser.UnknownIdentifierValidation;
import com.exa.lang.parsing.XALParser;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public class DMFGeneral extends DataManFactory {
	
	protected final Map<String, DataManFactory> dmFactories = new HashMap<>();
	
	protected String defaultType;

	public DMFGeneral(String defaultType, FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMUSetup dmuSetup) {
		super(filesRepos, dataSources, defaultDataSource, dmuSetup);
		this.defaultType = defaultType;
		
		dmuDmf = this;
	}
	
	public DMFGeneral(String defaultType, FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource, DMUSetup dmuSetup, UnknownIdentifierValidation uiv) {
		super(filesRepos, dataSources, defaultDataSource, dmuSetup, uiv);
		
		this.defaultType = defaultType;
		
		dmuDmf = this;
	}
	
	public DataManFactory getDMF(String type) { return dmFactories.get(type); }
	
	@Override
	public void initialize() {
		DataManFactory dmf = new DMFLibre(this); dmf.initialize();
		dmFactories.put(DMFN_LIBRE, dmf);
		
		dmf = new DMFSmart(this); dmf.initialize();
		dmFactories.put(DMFN_SMART, dmf);
		
		dmf = new DMFXLiteral(this); dmf.initialize();
		dmFactories.put(DMFN_XLITERAL, dmf);
		
		dmf = new DMFRowToField(this); dmf.initialize();
		dmFactories.put(DMFN_ROW_TO_FIELD, dmf);
		
		dmf = new DMFWebService(this); dmf.initialize();
		dmFactories.put(DMFN_WS, dmf);
		
		dmf = new DMFMap(this); dmf.initialize();
		dmFactories.put(DMFN_MAP, dmf);
		
		dmf = new DMFSpSql(this); dmf.initialize();
		dmFactories.put(DMFN_SP_SQL, dmf);
		
		dmf = new DMFSql(this);dmf.initialize();
		
		dmFactories.put(DMFN_SQL, dmf);
		dmFactories.put("tsql", dmf);
		dmFactories.put("transact-sql", dmf);
		dmFactories.put("t-sql", dmf);
		dmFactories.put("plsql", dmf);
		dmFactories.put("pl-sql", dmf);
		dmFactories.put("sql-server", dmf);
		dmFactories.put("sql-oracle", dmf);
		dmFactories.put("oracle", dmf);
	}
	
	public DataReader<?> getDataReader(XALParser parser, String type, String drName, DCEvaluatorSetup evSetup) throws ManagedException {
		if(type == null) return getDataReader(parser, drName, evSetup);
		
		DataManFactory dmf = dmFactories.get(type);
		
		if(dmf == null) throw new ManagedException(String.format("Invalid data reader type '%s' for '%s'", type, drName));
		
		return dmf.getDataReader(parser, drName, evSetup);
	}
	
	@Override
	public DataReader<?> getDataReader(String name, ObjectValue<XPOperand<?>> ovEntity, DMUtils dmu) throws ManagedException {
		String type = ovEntity.getAttributAsString("type", defaultType);
		
		DataManFactory dmf = dmFactories.get(type);
		
		if(dmf == null) throw new ManagedException(String.format("Invalid type '%s' for '%s'", type, name));
		
		return dmf.getDataReader(name, ovEntity, dmu);
	}

	@Override
	public DataWriter<?> getDataWriter(String name, ObjectValue<XPOperand<?>> ovEntity, DataReader<?> drSource, DMUtils dmu, boolean preventInsertion, boolean preventUpdate) throws ManagedException {
		String type = ovEntity.getAttributAsString("type", defaultType);
		
		DataManFactory dmf = dmFactories.get(type);
		
		if(dmf == null) throw new ManagedException(String.format("Invalid type '%s'", type));
		
		return dmf.getDataWriter(name, ovEntity, drSource, dmu, preventInsertion, preventUpdate);
	}

	
}
