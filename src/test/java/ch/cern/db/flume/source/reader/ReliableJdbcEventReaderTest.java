package ch.cern.db.flume.source.reader;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ch.cern.db.flume.source.reader.ReliableJdbcEventReader;

public class ReliableJdbcEventReaderTest {
	
	String connection_url = "jdbc:hsqldb:mem:aname";
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
	public void eventsFromDatabase(){
		
		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.CONNECTION_URL_PARAM, connection_url);
		context.put(ReliableJdbcEventReader.USERNAME_PARAM, "SA");
		context.put(ReliableJdbcEventReader.PASSWORD_PARAM, "");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, " audit_data_table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "ID");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "numeric");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader(context);
		
		try {
			Event event = reader.readEvent();
			Assert.assertNull(event);
			
			Statement statement = connection.createStatement();
			statement.execute("INSERT INTO audit_data_table VALUES 2, 48, 'name2';");
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":2,\"RETURN_CODE\":48,\"NAME\":\"name2\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNull(event);
			
			statement = connection.createStatement();
			statement.execute("INSERT INTO audit_data_table VALUES 1, 48, 'name1';");
			statement.execute("INSERT INTO audit_data_table VALUES 3, 48, 'name3';");
			statement.close();
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":1,\"RETURN_CODE\":48,\"NAME\":\"name1\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":2,\"RETURN_CODE\":48,\"NAME\":\"name2\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":3,\"RETURN_CODE\":48,\"NAME\":\"name3\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNull(event);
			
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void eventsFromDatabaseInBatchFasion(){
		
		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.CONNECTION_URL_PARAM, connection_url);
		context.put(ReliableJdbcEventReader.USERNAME_PARAM, "SA");
		context.put(ReliableJdbcEventReader.PASSWORD_PARAM, "");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, " audit_data_table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "ID");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "numeric");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader(context);
		
		try {
			List<Event> events = reader.readEvents(10);
			Assert.assertEquals(0, events.size());
			
			Statement statement = connection.createStatement();
			statement.execute("INSERT INTO audit_data_table VALUES 2, 48, 'name2';");
			events = reader.readEvents(10);
			Assert.assertNotNull(events);
			Assert.assertEquals(1, events.size());
			Assert.assertEquals("{\"ID\":2,\"RETURN_CODE\":48,\"NAME\":\"name2\"}", new String(events.get(0).getBody()));
			
			statement = connection.createStatement();
			statement.execute("INSERT INTO audit_data_table VALUES 1, 48, 'name1';");
			statement.execute("INSERT INTO audit_data_table VALUES 3, 48, 'name3';");
			statement.close();
			events = reader.readEvents(10);
			Assert.assertNotNull(events);
			Assert.assertEquals(3, events.size());
			Assert.assertEquals("{\"ID\":1,\"RETURN_CODE\":48,\"NAME\":\"name1\"}", 
					new String(events.get(0).getBody()));
			Assert.assertEquals("{\"ID\":2,\"RETURN_CODE\":48,\"NAME\":\"name2\"}", 
					new String(events.get(1).getBody()));
			Assert.assertEquals("{\"ID\":3,\"RETURN_CODE\":48,\"NAME\":\"name3\"}", 
					new String(events.get(2).getBody()));
			
			reader.commit();
			events = reader.readEvents(10);
			Assert.assertEquals(0, events.size());
			
			statement = connection.createStatement();
			statement.execute("INSERT INTO audit_data_table VALUES 4, 48, 'name4';");
			statement.execute("INSERT INTO audit_data_table VALUES 5, 48, 'name5';");
			statement.close();
			events = reader.readEvents(1);
			Assert.assertNotNull(events);
			Assert.assertEquals(1, events.size());
			Assert.assertEquals("{\"ID\":4,\"RETURN_CODE\":48,\"NAME\":\"name4\"}", 
					new String(events.get(0).getBody()));
			reader.commit();
			events = reader.readEvents(1);
			Assert.assertNotNull(events);
			Assert.assertEquals(1, events.size());
			Assert.assertEquals("{\"ID\":5,\"RETURN_CODE\":48,\"NAME\":\"name5\"}", 
					new String(events.get(0).getBody()));
			events = reader.readEvents(1);
			Assert.assertNotNull(events);
			Assert.assertEquals(0, events.size());
			
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void readFromDatabaseAndCommit(){
		
		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.CONNECTION_URL_PARAM, connection_url);
		context.put(ReliableJdbcEventReader.USERNAME_PARAM, "SA");
		context.put(ReliableJdbcEventReader.PASSWORD_PARAM, "");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, " audit_data_table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "ID");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "numeric");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader(context);
		
		try {
			//Remove commit file
			new File(ReliableJdbcEventReader.COMMITTING_FILE_PATH_DEFAULT).delete();
			
			Event event = reader.readEvent();
			Assert.assertNull(event);
			
			Statement statement = connection.createStatement();
			statement.execute("INSERT INTO audit_data_table VALUES 1, 48, 'name1';");
			statement.execute("INSERT INTO audit_data_table VALUES 2, 48, 'name2';");
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":1,\"RETURN_CODE\":48,\"NAME\":\"name1\"}", new String(event.getBody()));
			
			reader.close();
			reader = new ReliableJdbcEventReader(context);
			
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":1,\"RETURN_CODE\":48,\"NAME\":\"name1\"}", new String(event.getBody()));
			reader.commit();
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":2,\"RETURN_CODE\":48,\"NAME\":\"name2\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNull(event);
			
			reader.close();
			reader = new ReliableJdbcEventReader(context);
			
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":2,\"RETURN_CODE\":48,\"NAME\":\"name2\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNull(event);
			
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
	}

	@Test
	public void createCommitFile(){
		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader(context);
		
		String timestamp = "2016-02-09 09:34:51.244507 Europe/Zurich";
		
		reader.last_value = timestamp ;
		try {
			reader.commit();
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
		
		try {
			FileReader in = new FileReader(ReliableJdbcEventReader.COMMITTING_FILE_PATH_DEFAULT);
			char [] in_chars = new char[50];
		    in.read(in_chars);
			in.close();
			
			String timestamp_from_file = new String(in_chars).trim();
			
			Assert.assertEquals(timestamp, reader.committed_value);
			Assert.assertEquals(timestamp, timestamp_from_file);
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void readCommitFile(){
		
		String timestamp = "2016-02-09 09:34:51.244507 Europe/Zurich";
		
		try {
			FileWriter out = new FileWriter(ReliableJdbcEventReader.COMMITTING_FILE_PATH_DEFAULT, false);
			out.write(timestamp);
			out.close();
		} catch (IOException e) {
			Assert.fail();
		}
		
		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader(context);
		
		Assert.assertEquals(timestamp, reader.committed_value);
	}
	
	@Test
	public void readEmptyCommitFile(){
		
		//It will create the file
		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader(context);
		Assert.assertNull(reader.last_value);
		
		//It will read an empty file
		reader = new ReliableJdbcEventReader(context);
		Assert.assertNull(reader.last_value);
		
		String timestamp = "2016-02-09 09:34:51.244507 Europe/Zurich";
		reader.last_value = timestamp;
		try {
			reader.commit();
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
		
		try {
			FileReader in = new FileReader(ReliableJdbcEventReader.COMMITTING_FILE_PATH_DEFAULT);
			char [] in_chars = new char[50];
		    in.read(in_chars);
			in.close();
			
			String timestamp_from_file = new String(in_chars).trim();
			
			Assert.assertEquals(timestamp, reader.committed_value);
			Assert.assertEquals(timestamp, timestamp_from_file);
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void queryCreation(){
		
		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PARAM, "new query");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader(context);
		
		String result = reader.createQuery(null);
		Assert.assertEquals(result, "new query");
		
		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PARAM, 
				"SELECT * FROM table_name [WHERE column_name > '{$committed_value}'] ORDER BY column_name");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		reader = new ReliableJdbcEventReader(context);
		
		result = reader.createQuery(null);
		Assert.assertEquals(result, "SELECT * FROM table_name  ORDER BY column_name");
		
		
		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PARAM, 
				"SELECT * FROM table_name [WHERE column_name > '{$committed_value}'] ORDER BY column_name");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		reader = new ReliableJdbcEventReader(context);
		
		result = reader.createQuery("12345");
		Assert.assertEquals(result, "SELECT * FROM table_name  WHERE column_name > '12345'  ORDER BY column_name");
		
		
		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		context.put(ReliableJdbcEventReader.QUERY_PARAM, "new query");
		reader = new ReliableJdbcEventReader(context);
		
		result = reader.createQuery("value");
		Assert.assertEquals(result, "new query");
		
		
		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name1");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name1");
		reader = new ReliableJdbcEventReader(context);
		
		result = reader.createQuery(null);
		Assert.assertEquals(result, "SELECT * FROM table_name1 ORDER BY column_name1");
		
		
		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name2");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name2");
		reader = new ReliableJdbcEventReader(context);
		
		result = reader.createQuery("2016-02-09 09:34:51.244");
		Assert.assertEquals(result, "SELECT * FROM table_name2 "
				+ "WHERE column_name2 > TIMESTAMP '2016-02-09 09:34:51.244' "
				+ "ORDER BY column_name2");
		
		
		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name2");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name2");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "timestamp");
		reader = new ReliableJdbcEventReader(context);
		
		result = reader.createQuery("2016-02-09 09:34:51.244");
		Assert.assertEquals(result, "SELECT * FROM table_name2 "
				+ "WHERE column_name2 > TIMESTAMP '2016-02-09 09:34:51.244' "
				+ "ORDER BY column_name2");
		
		
		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name3");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name3");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "numeric");
		reader = new ReliableJdbcEventReader(context);
		
		result = reader.createQuery("244");
		Assert.assertEquals(result, "SELECT * FROM table_name3 "
				+ "WHERE column_name3 > 244 "
				+ "ORDER BY column_name3");
		
		
		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name4");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name4");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "string");
		reader = new ReliableJdbcEventReader(context);
		
		result = reader.createQuery("string4");
		Assert.assertEquals(result, "SELECT * FROM table_name4 "
				+ "WHERE column_name4 > \'string4\' "
				+ "ORDER BY column_name4");
		
		
		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name4");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name4");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "does_not_exist");
		
		try{
			reader = new ReliableJdbcEventReader(context);
			Assert.fail();
		}catch(FlumeException e){
		}
	}

	@After
	public void cleanUp(){
		new File(ReliableJdbcEventReader.COMMITTING_FILE_PATH_DEFAULT).delete();
		
		try {
			connection.close();
		} catch (SQLException e) {}
	}
}