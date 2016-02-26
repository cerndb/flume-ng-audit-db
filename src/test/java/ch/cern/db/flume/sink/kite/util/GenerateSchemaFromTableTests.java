package ch.cern.db.flume.sink.kite.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ch.cern.db.flume.sink.kite.util.GenerateSchemaFromTable;


public class GenerateSchemaFromTableTests {
	
	String connection_url = "jdbc:hsqldb:mem:testdb";
	Connection connection = null;
	
	@Before
	public void setup(){
		try {
			connection = DriverManager.getConnection(connection_url, "sa", "");
			
			Statement statement = connection.createStatement();
			statement.execute("DROP TABLE IF EXISTS audit_data_table;");
			statement.execute("CREATE TABLE audit_data_table (id INTEGER, return_code BIGINT, name VARCHAR(20));");
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}
	
	@Test
	public void generateSchema() throws SQLException{
		
		GenerateSchemaFromTable generator = new GenerateSchemaFromTable();
		generator.configure(new String[]{
				"-dc", "org.hsqldb.jdbc.JDBCDriver",
				"-c", connection_url,
				"-t", "AUDIT_DATA_TABLE",
				"-u", "sa",
				"-p", "",
				"-schema", "PUBLIC" //Can be removed
				});
		
		Schema schema = generator.getSchema();
		
		Assert.assertEquals("{\"type\":\"record\",\"name\":\"audit\",\"fields\":["
				+ "{\"name\":\"ID\",\"type\":[\"int\",\"null\"]},"
				+ "{\"name\":\"RETURN_CODE\",\"type\":[\"int\",\"null\"]},"
				+ "{\"name\":\"NAME\",\"type\":[\"string\",\"null\"]}]}", 
				schema.toString(false));
	}
	
	@Test(expected=SQLException.class)
	public void noTableFound() throws SQLException{
		
		GenerateSchemaFromTable generator = new GenerateSchemaFromTable();
		generator.configure(new String[]{
				"-dc", "org.hsqldb.jdbc.JDBCDriver",
				"-c", connection_url,
				"-t", "AUDIT_DATA_TABLE_no_exists",
				"-u", "sa",
				"-p", ""
				});
		
		generator.getSchema();
		Assert.fail();
	}
	
	@After
	public void cleanUp(){
		try {
			connection.close();
		} catch (SQLException e) {
		}
	}
}
