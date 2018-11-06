package com.exa.data;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.exa.data.config.DMFLibre;
import com.exa.data.config.DMFRowToField;
import com.exa.data.config.DMFSql;
import com.exa.data.config.DataManFactory;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.XPEvaluator;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.values.ObjectValue;

public abstract class StandardDRWithDSBase<_FIELD extends Field> extends StandardDataReaderBase<_FIELD> {
	public static final String DMFN_SQL = "sql";
	
	public static final String DMFN_SMART = "smart";
	
	public static final String DMFN_ROW_TO_FIELD = "row-to-field";
	
	public static final String DMFN_LIBRE = "libre";
	
	protected Map<String, DataManFactory> dmFactories = new HashMap<>();
	
	protected ObjectValue<XPOperand<?>> config;
	
	protected FilesRepositories filesRepos;
	
	protected Map<String, DataSource> dataSources;
	
	protected String defaultDataSource;
	
	public StandardDRWithDSBase(String name, ObjectValue<XPOperand<?>> config, XPEvaluator evaluator, FilesRepositories filesRepos, Map<String, DataSource> dataSources, String defaultDataSource) {
		super(name, evaluator);
		
		this.config = config;
		
		this.filesRepos = filesRepos;
		this.dataSources = dataSources;
		this.defaultDataSource = defaultDataSource;
		
		dmFactories.put(DMFN_LIBRE, new DMFLibre(filesRepos));
		
		dmFactories.put(DMFN_ROW_TO_FIELD, new DMFRowToField(filesRepos, dataSources, defaultDataSource));
		
		dmFactories.put(DMFN_SQL, new DMFSql(filesRepos, dataSources, defaultDataSource));
	}

	public StandardDRWithDSBase(String name, ObjectValue<XPOperand<?>> config, FilesRepositories filesRepos, Map<String, DataSource> dataSources, String defaultDataSource) {
		this(name, config, null, filesRepos, dataSources, defaultDataSource);
	}

}
