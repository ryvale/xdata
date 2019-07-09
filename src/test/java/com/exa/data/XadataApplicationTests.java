package com.exa.data;

import java.util.HashMap;
import java.util.Map;

import com.exa.data.config.DMFMap;
import com.exa.data.config.DMFSmart;
import com.exa.data.config.DMFSql;
import com.exa.data.config.DMFWebService;
import com.exa.data.config.DMFXLiteral;
import com.exa.data.config.DataManFactory;
import com.exa.data.expression.DCEvaluatorSetup;
import com.exa.data.sql.XASQLDataSource;
import com.exa.data.web.WSDataSource;
import com.exa.utils.ManagedException;
import com.exa.utils.io.FilesRepositories;
import com.exa.utils.io.OSFileRepoPart;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import junit.framework.TestCase;

public class XadataApplicationTests extends TestCase {
	
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
        ds.setServerName("192.168.136.143");  
        ds.setPortNumber(1433);   
        ds.setDatabaseName("EAMPROD");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DataManFactory dmf = new DMFSmart(filesRepo, dataSources, "default");
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        DataReader<?> dr = dmf.getDataReader("default:/test1", evSetup);
        
        dr.open();
        
        assertTrue(new Boolean(dr.next()));
        
        assertFalse(new Boolean(dr.next()));
        
        dr.close();
	}
	
	public void testSmart2() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("sa");  
        ds.setPassword("e@mP0wer");  
        ds.setServerName("192.168.23.129");  
        ds.setPortNumber(1433);
        ds.setDatabaseName("EAMPROD");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DataManFactory dmf = new DMFSmart(filesRepo, dataSources, "default");
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        evSetup.addVaraiable("start", String.class, "01/02/2016");
        evSetup.addVaraiable("end", String.class, "17/08/2018");
        
        DataReader<?> dr = dmf.getDataReader("default:/test2#entity2", evSetup);
        
        dr.open();
        
        assertTrue(new Boolean(dr.next()));
        
        System.out.println(dr.getInteger("nb1"));
        
        System.out.println(dr.getString("nb2"));
        
        assertFalse(new Boolean(dr.next()));
        
		dr.close();
	}
	
	public void testDwSQL() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("sa");  
        ds.setPassword("e@mP0wer");  
        ds.setServerName("192.168.23.129");  
        ds.setPortNumber(1433);
        ds.setDatabaseName("EAMPROD");
        
        Map<String, XADataSource> dataSources = new HashMap<>();
        dataSources.put("default", new XASQLDataSource(ds));
        
        DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
        
        DataManFactory dmfSQL = new DMFSql(filesRepo, dataSources, "default", (id, context) -> {
        	if("rootOv".equals(id)) return "ObjectValue";
        	
        	if("updateMode".equals(id)) return "string";
        	
        	return null;
        });//new DMFSmart(filesRepo, dataSources, "default");
        DataManFactory dmfXL = new DMFXLiteral(filesRepo, (id, context) -> {
        	if("updateMode".equals(id)) return "string";
        	
        	return null;
        });
        
        DataReader<?> drSource = dmfXL.getDataReader("default:/test4#testData", evSetup);
        
        DataWriter<?> dw = dmfSQL.getDataWriter("default:/test4#r5uoms", evSetup, drSource);
        
        dw.open();
        
        drSource.open();
        
        while(drSource.next()) {
        	dw.execute();
        }
        
        drSource.close();
        
		dw.close();
	}
	
	public void testWS() throws ManagedException {
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
	}
	
	public void testMapReader() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		
		Map<String, Object> mp = new HashMap<>();
		mp.put("f1", "a");
		mp.put("f2", "b");
		
		DataManFactory dmf =  new DMFMap(filesRepo, () -> mp);
		
		DCEvaluatorSetup evSetup = new DCEvaluatorSetup();
		
		DataReader<?> dr = dmf.getDataReader("default:/test6#test", evSetup);
		
		dr.open();
		
		dr.next();
		
		System.out.println(dr.getString("f1"));
		
		assertTrue("b".equals(dr.getString("f2")));
		
		dr.close();
	}
}
 