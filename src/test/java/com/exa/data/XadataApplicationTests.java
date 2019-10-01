package com.exa.data;

import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.exa.data.config.DMFGeneral;
import com.exa.data.config.DMFMap;
import com.exa.data.config.DMFSmart;
import com.exa.data.config.DMFSpSql;
import com.exa.data.config.DMFSql;
import com.exa.data.config.DMFXLiteral;
import com.exa.data.config.DataManFactory;
import com.exa.data.expression.DCEvaluatorSetup;
import com.exa.data.sql.SQLDataReader;
import com.exa.data.sql.SQLDataWriter;
import com.exa.data.sql.XASQLDataSource;
import com.exa.eva.OperatorManager.OMOperandType;
import com.exa.expression.OMMethod;
import com.exa.expression.eval.XPEvaluator;
import com.exa.lang.parsing.XALParser;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.io.OSFileRepoPart;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import junit.framework.TestCase;

public class XadataApplicationTests extends TestCase {
	
	static {
		SQLDataReader.debugOn = true;
		SQLDataWriter.debugOn = true;
	}
	
	public XadataApplicationTests( String testName ) {
        super( testName );
    }
	
	public void testSmart1() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("sa");  
        ds.setPassword("e@mP0wer");  
        ds.setServerName("192.168.255.128");  
        ds.setPortNumber(1433);   
        ds.setDatabaseName("EAMPROD");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DataManFactory dmf = new DMFSmart(filesRepo, dataSources, "default", s -> {});
        dmf.initialize();
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        XALParser parser = new XALParser();
        
        DataReader<?> dr = dmf.getDataReader(parser, "default:/smart1", evSetup);
        
        dr.open();
        
        assertTrue(new Boolean(dr.next()));
        
        assertFalse(new Boolean(dr.next()));
        
        dr.close();
	}
	
	
	public void testDMFGeneral() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("sa");  
        ds.setPassword("e@mP0wer");  
        ds.setServerName("192.168.255.128");  
        ds.setPortNumber(1433);
        ds.setDatabaseName("EAMPROD");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DataManFactory dmf = new DMFGeneral(DataManFactory.DMFN_SMART, filesRepo, dataSources, "default", s -> {});
        dmf.initialize();
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        XALParser parser = new XALParser();
        
        DataReader<?> dr= dmf.getDataReader(parser, "default:/smart1", evSetup);
        
        dr.open();
        
        assertTrue(new Boolean(dr.next()));
        
        assertFalse(new Boolean(dr.next()));
        
        dr.close();
        
        dr = dmf.getDataReader(parser, "default:/sp-sql#newWOCode", evSetup);
        
        dr.open();
        
        System.out.println(dr.getString("value"));
        dr.close();
	}
	
	public void testSmart2() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("sa");  
        ds.setPassword("e@mP0wer");  
        ds.setServerName("192.168.255.128");  
        ds.setPortNumber(1433);
        ds.setDatabaseName("EAMPROD");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DataManFactory dmf = new DMFSmart(filesRepo, dataSources, "default", s -> {});
        dmf.initialize();
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        evSetup.addVaraiable("start", String.class, "01/02/2016");
        evSetup.addVaraiable("end", String.class, "17/08/2018");
        
        XALParser parser =  new XALParser();
        
        DataReader<?> dr = dmf.getDataReader(parser, "default:/smart2#entity2", evSetup);
        
        dr.open();
        
        assertTrue(new Boolean(dr.next()));
        
        System.out.println(dr.getInteger("nb1"));
        
        System.out.println(dr.getString("nb2"));
        
        assertFalse(new Boolean(dr.next()));
        
		dr.close();
	}
	
	/*public void testRealCase() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("private", new OSFileRepoPart("./src/test/java/com/exa/data/private"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		
		DriverManagerDataSource ds = new DriverManagerDataSource();
		ds.setDriverClassName("oracle.jdbc.OracleDriver");
		
		String cnStr = "jdbc:oracle:thin:@//10.108.28.23:1521/GMAOP"; //getConnectionString("", "1521", dataBaseName, extendedProperties);
		ds.setUrl(cnStr);
		ds.setUsername("UGMAO");
		ds.setPassword("ugmao");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DataManFactory dmf = new DMFSmart(filesRepo, dataSources, "default", s -> {});
        dmf.initialize();
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        evSetup.addVaraiable("start", String.class, "15/08/2019");
        evSetup.addVaraiable("end", String.class, "30/08/2019");
        
        DataReader<?> dr = dmf.getDataReader("private:/stats-sollicitation#dras", evSetup);
        
        dr.open();
        
        assertTrue(new Boolean(dr.next()));
        
        System.out.println(dr.getInteger("traite022"));
        
        //System.out.println(dr.getString("nb2"));
        
        assertFalse(new Boolean(dr.next()));
        
		dr.close();
	}*/
	
	public void testSQL() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("sa");  
        ds.setPassword("e@mP0wer");  
        ds.setServerName("192.168.255.128");  
        ds.setPortNumber(1433);
        ds.setDatabaseName("EAMPROD");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DataManFactory dmf = new DMFSql(filesRepo, dataSources, "default", s -> {}); // new DMFSmart(filesRepo, dataSources, "default", s -> {});
        dmf.initialize();
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        evSetup.addVaraiable("start", String.class, "01/02/2016");
        evSetup.addVaraiable("end", String.class, "17/08/2018");
        
        
       XALParser parser = new XALParser();
        
        DataReader<?> dr = dmf.getDataReader(parser, "default:/sql#r5uoms", evSetup);
        
        dr.open();
        
        assertTrue(new Boolean(dr.next()));
        
		dr.close();
	}
	
	public void testDwSQL() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("sa");  
        ds.setPassword("e@mP0wer");  
        ds.setServerName("192.168.255.128");  
        ds.setPortNumber(1433);
        ds.setDatabaseName("EAMPROD");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        DataManFactory dmfSQL = new DMFSql(filesRepo, dataSources, "default", s -> {}, (id, context) -> {
        	if("rootOv".equals(id)) return "ObjectValue";
        	
        	if("updateMode".equals(id)) return "string";
        	
        	if("dmu".equals(id)) return "DMUtils";
        	
        	return null;
        });//new DMFSmart(filesRepo, dataSources, "default");
        dmfSQL.initialize();
        DataManFactory dmfXL = new DMFXLiteral(filesRepo, dataSources, "default", s -> {}, (id, context) -> {
        	if("updateMode".equals(id)) return "string";
        	
        	if("dmu".equals(id)) return "DMUtils";
        	
        	return null;
        });
        dmfXL.initialize();
        
        XALParser parser = new XALParser();
        
        DataReader<?> drSource = dmfXL.getDataReader(parser, "default:/sql-w#testData", evSetup);
        
        DataWriter<?> dw = dmfSQL.getDataWriter(parser, "default:/sql-w#r5uoms", evSetup, drSource, false, false);
        
        drSource.open();
        
        dw.open();
        
        while(drSource.next()) {
        	dw.execute();
        }
        
        drSource.close();
        
		dw.close();
	}
	
	public void testDwFromStringSQL() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("sa");  
        ds.setPassword("e@mP0wer");  
        ds.setServerName("192.168.255.128");  
        ds.setPortNumber(1433);
        ds.setDatabaseName("EAMPROD");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        DataManFactory dmfSQL = new DMFSql(filesRepo, dataSources, "default", s -> {}, (id, context) -> {
        	if("rootOv".equals(id)) return "ObjectValue";
        	
        	if("updateMode".equals(id)) return "string";
        	
        	if("dmu".equals(id)) return "DMUtils";
        	
        	return null;
        });//new DMFSmart(filesRepo, dataSources, "default");
        dmfSQL.initialize();
        DataManFactory dmfXL = new DMFXLiteral(filesRepo, dataSources, "default", s -> {}, (id, context) -> {
        	if("updateMode".equals(id)) return "string";
        	
        	if("dmu".equals(id)) return "DMUtils";
        	
        	return null;
        });
        dmfXL.initialize();
        
        XALParser parser = new XALParser();
        
        DataReader<?> drSource = dmfXL.getDataReader(parser, "default:/sql-w#testData2", evSetup);
        
        DataWriter<?> dw = dmfSQL.getDataWriter(parser, "default:/sql-w#r5events", evSetup, drSource, false, false);
        
        drSource.open();
        
        dw.open();
        
        while(drSource.next()) {
        	dw.execute();
        }
        
        drSource.close();
        
		dw.close();
	}
	
	public void testStoredProcSQL() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("sa");  
        ds.setPassword("e@mP0wer");  
        ds.setServerName("192.168.255.128");  
        ds.setPortNumber(1433);
        ds.setDatabaseName("EAMPROD");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DataManFactory dmf = new DMFSpSql(filesRepo, dataSources, "default", s -> {});
        dmf.initialize();
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        XALParser parser = new XALParser();
        
        DataReader<?> dr = dmf.getDataReader(parser, "default:/sp-sql#newWOCode", evSetup);
        
        dr.open();
        
        System.out.println(dr.getString("value"));
        dr.close();
	}
	
	/*public void testWS() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		
		Map<String, XADataSource> dataSources = new HashMap<>();
		dataSources.put("default", new WSDataSource("http://localhost:8082/i-eam-jade/"));
		
		DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
		evSetup.addVaraiable("login", String.class, "admin");
        evSetup.addVaraiable("pwd", String.class, "admin");
        evSetup.addVaraiable("tokenRequired", Boolean.class, Boolean.TRUE);
		
		DataManFactory dmfWS = new DMFWebService(filesRepo, dataSources, "default", (id, context) -> {
        	if("rootOv".equals(id)) return "ObjectValue";
        	
        	if("rootDr".equals(id)) return "DataReader";
        	
        	return null;
        });
		
		DataReader<?> dr = dmfWS.getDataReader("default:/test5#loginParam", evSetup);
		
		dr.open();
		
		dr.next();
		
		System.out.println(dr.getString("status"));
		
		System.out.println(dr.getString("profile"));
		
		System.out.println(dr.getString("token"));
		
		dr.close();
	}*/
	
	public void testMapReader() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		
		Map<String, Object> mp = new HashMap<>();
		mp.put("f1", "a");
		mp.put("f2", "b");
		
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new MapDataSource(() -> mp));
		
		DataManFactory dmf =  new DMFMap(filesRepo, dataSources, "default", s -> {});
		dmf.initialize();
		
		DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
		
		XALParser parser = new XALParser();
		
		DataReader<?> dr = dmf.getDataReader(parser, "default:/map#test", evSetup);
		
		dr.open();
		
		dr.next();
		
		System.out.println(dr.getString("f1"));
		
		assertTrue("b".equals(dr.getString("f2")));
		
		dr.close();
	}
	
	public void testMacroSP() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("sa");  
        ds.setPassword("e@mP0wer");  
        ds.setServerName("192.168.255.128");  
        ds.setPortNumber(1433);
        ds.setDatabaseName("EAMPROD");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        DataManFactory dmfGen = new DMFGeneral("smart", filesRepo, dataSources, "default", s -> {});//new DMFSmart(filesRepo, dataSources, "default");
        dmfGen.initialize();
        
        XALParser parser = new XALParser();
        
        DataReader<?> dr = dmfGen.getDataReader(parser, "default:/macro", evSetup);
        
        dr.open();
        
        dr.next();
        
        System.out.println(dr.getString("code"));
        
		dr.close();
	}
	
	public void testBeforeConnectionActions() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		Map<String, XADataSource> dataSources = new HashMap<>();
		
		
		DCEvaluatorSetup evSetup = new DCEvaluatorSetup() {

			@Override
			public void setup(XPEvaluator evaluator) throws ManagedException {
				OMMethod<String> omStr = new OMMethod<>("executeFlow", 2, OMOperandType.POST_OPERAND);
				omStr.addOperator(new MtdExecuteFlow());
				
				T_DMU.register(omStr, String.class);
				super.setup(evaluator);
			}
			
			
		};
		
		DataManFactory dmfGen = new DMFGeneral("smart", filesRepo, dataSources, "default", s -> {});//new DMFSmart(filesRepo, dataSources, "default");
	    dmfGen.initialize();
	    
	    XALParser parser = new XALParser();
	    
	    DataReader<?> dr = dmfGen.getDataReader(parser, "default:/bca#entity1", evSetup);
        
        dr.open();
        
        dr.next();
        
        System.out.println(dr.getString("code"));
        
		dr.close();
	}
	
	public void testDMUNewMethod() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		DCEvaluatorSetup evSetup = new DCEvaluatorSetup() {

			@Override
			public void setup(XPEvaluator evaluator) throws ManagedException {
				OMMethod<String> omStr = new OMMethod<>("executeFlow", 2, OMOperandType.POST_OPERAND);
				omStr.addOperator(new MtdExecuteFlow());
				
				T_DMU.register(omStr, String.class);
				super.setup(evaluator);
			}
			
		};
		
		Map<String, XADataSource> dataSources = new HashMap<>();
		DataManFactory dmfGen = new DMFGeneral("smart", filesRepo, dataSources, "default", s -> {});//new DMFSmart(filesRepo, dataSources, "default");
	    dmfGen.initialize();
	    
	    XALParser parser = new XALParser();
	    
	    DataReader<?> dr = dmfGen.getDataReader(parser, "default:/dmu-setup", evSetup);
        
        dr.open();
        
        dr.next();
        
        assertTrue("OK".equals(dr.getString("code")));
        assertTrue("test".equals(dr.getString("message")));
        
		dr.close();
		
	}
}
 