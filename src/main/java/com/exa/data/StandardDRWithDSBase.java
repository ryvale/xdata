package com.exa.data;

import java.util.HashMap;
import java.util.Map;

import com.exa.data.MapReader.MapGetter;
import com.exa.data.config.DMFLibre;
import com.exa.data.config.DMFMap;
import com.exa.data.config.DMFRowToField;
import com.exa.data.config.DMFSpSql;
import com.exa.data.config.DMFSql;
import com.exa.data.config.DMFWebService;
import com.exa.data.config.DMFXLiteral;
import com.exa.data.config.DataManFactory;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public abstract class StandardDRWithDSBase<_FIELD extends Field> extends StandardDataReaderBase<_FIELD> {
	public static final String DMFN_SQL = "sql";
	
	public static final String DMFN_SMART = "smart";
	
	public static final String DMFN_ROW_TO_FIELD = "row-to-field";
	
	public static final String DMFN_LIBRE = "libre";
	
	public static final String DMFN_XLITERAL = "x-literal";
	
	public static final String DMFN_WS = "ws";
	
	public static final String DMFN_MAP = "map";
	
	public static final String DMFN_SP_SQL = "sp-sql";
	
	protected Map<String, DataManFactory> dmFactories = new HashMap<>();
	
	protected ObjectValue<XPOperand<?>> config;
	
	protected FilesRepositories filesRepos;
	
	protected Map<String, XADataSource> dataSources;
	
	protected String defaultDataSource;
	
	public StandardDRWithDSBase(String name, ObjectValue<XPOperand<?>> config, XPEvaluator evaluator, VariableContext variableContext, FilesRepositories filesRepos, Map<String, XADataSource> dataSources, String defaultDataSource) {
		super(name, evaluator, variableContext);
		
		this.config = config;
		
		this.filesRepos = filesRepos;
		this.dataSources = dataSources;
		this.defaultDataSource = defaultDataSource;
		
		dmFactories.put(DMFN_LIBRE, new DMFLibre(filesRepos));
		
		dmFactories.put(DMFN_XLITERAL, new DMFXLiteral(filesRepos));
		
		dmFactories.put(DMFN_ROW_TO_FIELD, new DMFRowToField(filesRepos, dataSources, defaultDataSource));
		
		dmFactories.put(DMFN_WS, new DMFWebService(filesRepos, dataSources, defaultDataSource));
		
		dmFactories.put(DMFN_MAP, new DMFMap(filesRepos, dataSources, defaultDataSource));
		
		dmFactories.put(DMFN_SP_SQL, new DMFSpSql(filesRepos, dataSources, defaultDataSource));
		
		DataManFactory dmf = new DMFSql(filesRepos, dataSources, defaultDataSource);
		
		dmFactories.put(DMFN_SQL, dmf);
		dmFactories.put("tsql", dmf);
		dmFactories.put("plsql", dmf);
		dmFactories.put("sql-server", dmf);
		dmFactories.put("sql-oracle", dmf);
		dmFactories.put("oracle", dmf);
	}

	/*public StandardDRWithDSBase(String name, ObjectValue<XPOperand<?>> config, FilesRepositories filesRepos, Map<String, DataSource> dataSources, String defaultDataSource) {
		this(name, config, null, filesRepos, dataSources, defaultDataSource);
	}*/

}
