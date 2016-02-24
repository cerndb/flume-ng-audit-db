package ch.cern.db.audit.flume.sink.kite.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class GenerateSchemaFromTableTests {
	
	String connection_url = "jdbc:hsqldb:mem:testdb";
	Connection connection = null;
	
	@Before
	public void setup(){
		try {
			connection = DriverManager.getConnection(connection_url, "sa", "");
			
			Statement statement = connection.createStatement();
			statement.execute("CREATE SCHEMA TEST;");
			statement.execute("DROP TABLE IF EXISTS TEST.audit_data_table;");
			statement.execute("CREATE TABLE TEST.audit_data_table (id INTEGER, return_code BIGINT, name VARCHAR(20));");
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}
	
	@Test
	public void generateSchema() throws SQLException{
		
		GenerateSchemaFromTable generator = new GenerateSchemaFromTable();
		generator.configure(new String[]{
				"-c", connection_url,
				"-t", "AUDIT_DATA_TABLE",
				"-u", "sa",
				"-p", "",
				"-schema", "null",
				"-catalog", "null"
				});
		
		Schema schema = generator.getSchema();
		
		Assert.assertEquals("{\"type\":\"record\",\"name\":\"audit\",\"fields\":["
				+ "{\"name\":\"ID\",\"type\":[\"int\",\"null\"]},"
				+ "{\"name\":\"RETURN_CODE\",\"type\":[\"int\",\"null\"]},"
				+ "{\"name\":\"NAME\",\"type\":[\"string\",\"null\"]}]}", 
				schema.toString(false));
	}
	
	@After
	public void cleanUp(){
		try {
			connection.close();
		} catch (SQLException e) {
		}
	}
}
