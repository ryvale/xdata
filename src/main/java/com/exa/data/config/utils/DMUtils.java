package com.exa.data.config.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.exa.data.DataReader;
import com.exa.data.XADataSource;
import com.exa.data.action.ASAssignment;
import com.exa.data.action.Action;
import com.exa.data.action.ActionSeeker;
import com.exa.data.config.DMFGeneral;
import com.exa.data.config.DataManFactory.DMUSetup;
import com.exa.data.expression.macro.MCReaderStrValue;
import com.exa.data.expression.macro.Macro;
import com.exa.data.sql.SQLDataReader;
import com.exa.data.sql.SQLDataWriter;
import com.exa.data.sql.XASQLDataSource;
import com.exa.expression.VariableContext;
import com.exa.expression.XPOperand;
import com.exa.expression.eval.MapVariableContext;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.parsing.Computing;

import com.exa.utils.ManagedException;
import com.exa.utils.values.ObjectValue;
import com.exa.utils.values.Value;

public class DMUtils {
	public static Boolean FIELD_DEBUG = Boolean.FALSE;
	
	public static final DateFormat DF_STD = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static final String MC_READER_STR_VALUE = "reader-str-value";
	
	private Map<String, DataReader<?>> readers = new HashMap<>();
	
	protected Map<String, ?> params = new HashMap<>();
	
	private Map<String, DataReader<?>> drToClose = new LinkedHashMap<>();
	
	private Map<String, Connection> connectionsToClose = new HashMap<>();
	
	private List<Action> beforeConnectionActions = new ArrayList<>();
	
	private List<Action> onExecutionStartedActions = new ArrayList<>();
	
	private List<ActionSeeker> actionSeekers = new ArrayList<>();
	
	private DMFGeneral dmf;
	
	private Map<String, Macro<?>> macros = new HashMap<>();
	
	private VariableContext vc;
	
	private DMUSetup dmuSetup;
	
	private String dataSource;
	
	private boolean shouldCloseConnection = true;
	
	private XASQLDataSource xaSqlataSource = null;
	
	private Computing executedComputing;
	
	public DMUtils(DMFGeneral dmf, Computing executedComputing, VariableContext vc, DMUSetup dmuSetup, String dataSource) {
		super();
		//this.ovRoot = ovRoot;
		this.dmf = dmf;
		this.executedComputing = executedComputing;
		this.vc = vc;
		
		this.dmuSetup = dmuSetup;
		
		macros.put(MC_READER_STR_VALUE, new MCReaderStrValue(this));
		
		actionSeekers.add(new ASAssignment(this));
		
		this.dataSource = dataSource == null ? dmf.getDefaultDataSource() : dataSource;
	}
	
	public void register(String name, DataReader<?> dr) {
		readers.put(name, dr);
	}
	
	public void register(String type, Macro<?> macro) {
		macros.put(type, macro);
	}
	
	public void registerBeforeAction(Action action) {
		beforeConnectionActions.add(action);
	}
	
	public Action registerBeforeConnectionAction(String name, Value<?, XPOperand<?>> value) {
		for(ActionSeeker as : actionSeekers) {
			Action res = as.found(name, value);
			
			if(res == null) continue;
			
			beforeConnectionActions.add(res);
			
			return res;
		}
		
		return null;
	}
	
	public Action registerOnExecutionStartedAction(String name, Value<?, XPOperand<?>> value) {
		for(ActionSeeker as : actionSeekers) {
			Action res = as.found(name, value);
			
			if(res == null) continue;
			
			onExecutionStartedActions.add(res);
			
			return res;
		}
		
		return null;
	}

	public Computing getExecutedComputing() {
		return executedComputing;
	}

	public DataReader<?> getReader(String name) { return readers.get(name); }

	public Map<String, DataReader<?>> getReaders() { return readers; }
	
	public String evalString(String macroRef) throws ManagedException {
		ObjectValue<XPOperand<?>> ovRoot = executedComputing.getResult();
		ObjectValue<XPOperand<?>> ovMacro = ovRoot.getPathAttributAsObjecValue(macroRef);
		
		if(ovMacro == null) throw new ManagedException(String.format("'%s' macro is not defined", macroRef));
		
		String macroType = ovMacro.getRequiredAttributAsString("type");
		
		Macro<?> macro = macros.get(macroType);
		if(macro == null) throw new ManagedException(String.format("macro type '%s' is not defined", macroType));
		
		Macro<String> typMacro = macro.asMacroString();
		
		return typMacro.value(macroRef, ovMacro);
	}
	
	
	public DataReader<?> evalDataReader(String macroRef) throws ManagedException {
		ObjectValue<XPOperand<?>> ovRoot = executedComputing.getResult();
		ObjectValue<XPOperand<?>> ovMacro = ovRoot.getPathAttributAsObjecValue(macroRef);
		
		if(ovMacro == null) throw new ManagedException(String.format("'%s' macro is not defined", macroRef));
		
		String macroType = ovMacro.getRequiredAttributAsString("type");
		
		Macro<?> macro = macros.get(macroType);
		if(macro == null) throw new ManagedException(String.format("macro type '%s' is not defined", macroType));
		
		Macro<DataReader<?>> typMacro = macro.asMacroDataReader();
		
		return typMacro.value(macroRef, ovMacro);
	}

	public Map<String, ?> getParams() {
		return params;
	}

	public void setParams(Map<String, ?> params) {
		this.params = params;
	}

	public DMFGeneral getDmf() { return dmf; }

	public VariableContext getVc() { return vc;	}

	public XPEvaluator getEvaluator() {	return executedComputing.getXPEvaluator(); }
	
	public DMUtils newSubDmu(VariableContext vc, String ds) { return new DMUtils(dmf, executedComputing,  vc, dmuSetup, ds); }
	
	public DMUtils newSubDmu(String ds) { 
		VariableContext subVC =  new MapVariableContext(vc);
		return new DMUtils(dmf, executedComputing, subVC, dmuSetup, ds); 
	}
	
	public DataReader<?> loadReader(String readerRef) throws ManagedException {
		DataReader<?> oldDr = drToClose.get(readerRef);
		if(oldDr != null) try { oldDr.close(); drToClose.remove(readerRef); } catch (Exception e) {e.printStackTrace();}
		
		ObjectValue<XPOperand<?>> ovRoot = executedComputing.getResult();
		
		ObjectValue<XPOperand<?>> ovEntity = ovRoot.getPathAttributAsObjecValue(String.format("entities.%s", readerRef));
		if(ovEntity == null) throw new ManagedException(String.format("The path '%s' could be found", readerRef));
		String ds = ovEntity.getAttributAsString("dataSource", dmf.getDefaultDataSource());
		
		DMUtils dmu = newSubDmu(ds);
		dmuSetup.setup(dmu);
		
		ObjectValue<XPOperand<?>> ovReader = executedComputing.object(ovEntity, dmu.getVc());
		
		DataReader<?> res = dmf.getDataReader(readerRef, ovReader, dmu);
		
		try {res.open(); res.next();} 
		catch (Exception e) {	
			try { res.close();} catch (Exception e2) { e2.printStackTrace();	}
			throw new ManagedException(e);
		}
		drToClose.put(readerRef, res);
		
		return res;
	}
	
	public static Date parseDate(String strDate) throws ParseException {
		return DF_STD.parse(strDate);
	}
	
	public static Date parseDateWithFormat(String strDate, String strDateFormat) throws ParseException {
		DateFormat df = new SimpleDateFormat(strDateFormat);
		return df.parse(strDate);
	}
	
	public DataReader<?> openReader(String readerRef) throws ManagedException {
		DataReader<?> oldDr = drToClose.get(readerRef);
		if(oldDr != null) try { oldDr.close(); drToClose.remove(readerRef); } catch (Exception e) {e.printStackTrace();}
		
		ObjectValue<XPOperand<?>> ovRoot = executedComputing.getResult();
		
		ObjectValue<XPOperand<?>> ovEntity = ovRoot.getPathAttributAsObjecValue(String.format("entities.%s", readerRef));
		if(ovEntity == null) throw new ManagedException(String.format("The path '%s' could be found", readerRef));
		String ds = ovEntity.getAttributAsString("dataSource", dmf.getDefaultDataSource());
		DMUtils dmu = newSubDmu(ds);
		
		ObjectValue<XPOperand<?>> ovReader = executedComputing.object(ovEntity, dmu.getVc());//Computing.object(DataManFactory.parser, ovEntity, evaluator, vc, Computing.getDefaultObjectLib(ovRoot));
		
		DataReader<?> res = dmf.getDataReader(readerRef, ovReader, dmu);
		
		try { res.open(); } 
		catch (Exception e) {	
			try { res.close();} catch (Exception e2) { e2.printStackTrace(); }
			throw new ManagedException(e);
		}
		
		drToClose.put(readerRef, res);
		
		res.open();
		
		return res;
	}

	public void executeBeforeConnectionActions() throws ManagedException {
		for(Action action : beforeConnectionActions) {
			String res = action.execute();
			if("OK".equals(res) || res.startsWith("OK:")) continue;
			
			if(res.startsWith("ERROR:")) throw new ManagedException(res.substring(6));
			break;
		}
	}
	
	public void executeOnExecutionStarted() throws ManagedException {
		for(Action action : onExecutionStartedActions) {
			String res = action.execute();
			if("OK".equals(res) || res.startsWith("OK:")) continue;
			if(res.startsWith("ERROR:")) throw new ManagedException(res.substring(6));
			break;
		}
	}
	
	public Connection openSqlConnection(String cnName) throws ManagedException, SQLException {
		XADataSource xaDS = dmf.getDataSources().get(cnName);
		if(xaDS == null) throw new ManagedException(String.format("The data source %s specified is not present.", cnName));
		
		XASQLDataSource xasqlds = xaDS.asXASQLDataSource();
		if(xasqlds == null) throw new ManagedException(String.format("The data source %s specified should be sql type.", cnName));
		
		Connection cn = xasqlds.getNewConnection();
		
		connectionsToClose.put(cnName, cn);
		
		return cn;
	}
	
	public Connection openSqlConnection() throws ManagedException, SQLException { 
		if(xaSqlataSource == null) {
			XADataSource xaDS = dmf.getDataSources().get(dataSource);
			if(xaDS == null) throw new ManagedException(String.format("The data source %s specified is not present.", dataSource));
			
			xaSqlataSource = xaDS.asXASQLDataSource();
			if(dataSource == null) throw new ManagedException(String.format("The data source %s specified should be sql type.", dataSource));
		}
		Connection cn = xaSqlataSource.getNewConnection();
		
		connectionsToClose.put(dataSource, cn);
		
		return cn;
	}
	
	public Connection getSharedConnection(String cnName) throws ManagedException, SQLException {
		XADataSource xaDS = dmf.getDataSources().get(cnName);
		if(xaDS == null) throw new ManagedException(String.format("The data source %s specified is not present.", cnName));
		
		XASQLDataSource xasqlds = xaDS.asXASQLDataSource();
		if(xasqlds == null) throw new ManagedException(String.format("The data source %s specified should be sql type.", cnName));
		
		return xasqlds.getSharedConnection();
	}
	
	public Connection getSharedConnection() throws ManagedException, SQLException {
		if(xaSqlataSource == null) {
			XADataSource xaDS = dmf.getDataSources().get(dataSource);
			if(xaDS == null) throw new ManagedException(String.format("The data source %s specified is not present.", dataSource));
			
			xaSqlataSource = xaDS.asXASQLDataSource();
			if(dataSource == null) throw new ManagedException(String.format("The data source %s specified should be sql type.", dataSource));
		}
		return xaSqlataSource.getSharedConnection();
	}
	
	public void clean() {
		for(Connection cn: connectionsToClose.values()) {
			try {cn.close();} catch(Exception e) {}
		}
		for(DataReader<?> dr : drToClose.values()) {
			try {dr.close();} catch(Exception e) {}
		}
		drToClose.clear();
	}

	
	public boolean isShouldCloseConnection() {
		return shouldCloseConnection;
	}

	public void setShouldCloseConnection(boolean shouldCloseConnection) {
		this.shouldCloseConnection = shouldCloseConnection;
	}
	
	public Connection getSqlConnection() throws ManagedException, SQLException {
		if(shouldCloseConnection) return openSqlConnection();
		
		return getSharedConnection();
	}
	
	public void releaseSqlConnection() throws ManagedException {
		if(xaSqlataSource == null) {
			XADataSource xaDS = dmf.getDataSources().get(dataSource);
			if(xaDS == null) throw new ManagedException(String.format("The data source %s specified is not present.", dataSource));
			
			xaSqlataSource = xaDS.asXASQLDataSource();
			if(dataSource == null) throw new ManagedException(String.format("The data source %s specified should be sql type.", dataSource));
		}
		
		if(shouldCloseConnection) {
			Connection cn = connectionsToClose.get(dataSource);
			if(cn != null) {
				try {
					cn.close();
					if(SQLDataReader.debugOn || SQLDataWriter.debugOn) System.out.println("Connection closed id-" + cn.hashCode());
					connectionsToClose.remove(dataSource);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			return;
		}
		
		xaSqlataSource.releaseSharedConnection();
	}
	
	public static String toSQLString(List<?> values) {
		if(values.size() == 0) return "('')";
		
		StringBuilder sb = new StringBuilder();
		
		for(Object v : values) {
			sb.append(", '").append(v.toString().replaceAll("'", "''")).append("'");
		}
		
		return "(" + sb.substring(2) + ")";
	}
	
	public static String toSQLString(String v, String defaultValue) {
		
		if(v == null || "".equals(v)) return defaultValue;
		
		return "'" + v.replaceAll("'", "''") + "'";
	}
	
	public static String comaStringtoSQLString(String v, String defaultValue) {
		
		if(v == null) return null;
		
		String[] parts = v.split("[,]");
		
		StringBuilder res = new StringBuilder();
		
		for(String part : parts) {
			res.append(", " + toSQLString(part, defaultValue));
		}
		
		return  (res.length() == 0 ? "''" : res.substring(2));
	}
}
