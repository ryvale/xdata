package com.exa.data;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.exa.data.config.DMFSmart;
import com.exa.data.config.DataManFactory;
import com.exa.data.expression.RMEvaluatorSetup;
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
        
        Map<String, DataSource> dataSources = new HashMap<>();
        dataSources.put("default", ds);
        
        DataManFactory dmf = new DMFSmart(filesRepo, dataSources, "default");
        
        RMEvaluatorSetup evSetup = new RMEvaluatorSetup();
        
        DataReader<?> dr = dmf.getDataReader("default:/test1", evSetup);
        
        dr.open();
        
        assertTrue(new Boolean(dr.next()));
        
        assertFalse(new Boolean(dr.next()));
        
        
		
	}
	
	public void testSmart2() throws ManagedException {
		FilesRepositories filesRepo = new FilesRepositories();
		
		filesRepo.addRepoPart("default", new OSFileRepoPart("./src/test/java/com/exa/data"));
		filesRepo.addRepoPart("data-config", new OSFileRepoPart("C:/Users/leader/Desktop/travaux"));
		
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("sa");  
        ds.setPassword("e@mP0wer");  
        ds.setServerName("192.168.136.143");  
        ds.setPortNumber(1433);   
        ds.setDatabaseName("EAMPROD");
        
        Map<String, DataSource> dataSources = new HashMap<>();
        dataSources.put("default", ds);
        
        DataManFactory dmf = new DMFSmart(filesRepo, dataSources, "default");
        
        RMEvaluatorSetup evSetup = new RMEvaluatorSetup();
        
        evSetup.addVaraiable("start", String.class, "01/02/2016");
        evSetup.addVaraiable("end", String.class, "17/08/2018");
        
        DataReader<?> dr = dmf.getDataReader("default:/test2#entity2", evSetup);
        
        dr.open();
        
        assertTrue(new Boolean(dr.next()));
        
        System.out.println(dr.getInteger("nb1"));
        
        System.out.println(dr.getString("nb2"));
        
        assertFalse(new Boolean(dr.next()));
        
		
	}
}
 