/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
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
import org.apache.flume.conf.ConfigurationException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
	public void notConfiguredRaeder() throws IOException{
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();

		try{
			reader.readEvent();

			Assert.fail();
		}catch(ConfigurationException e){}

		try{
			reader.readEvents(10);

			Assert.fail();
		}catch(ConfigurationException e){}

		try{
			reader.commit();

			Assert.fail();
		}catch(ConfigurationException e){}

		try{
			reader.createQuery(null);

			Assert.fail();
		}catch(ConfigurationException e){}
	}

	@Test
	public void passwordByCommand(){

		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.CONNECTION_URL_PARAM, connection_url);
		context.put(ReliableJdbcEventReader.USERNAME_PARAM, "SA");
		context.put(ReliableJdbcEventReader.PASSWORD_PARAM, "");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, " audit_data_table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "ID");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "numeric");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);
		
		try {
			reader.readEvent();
		} catch (IOException e) {
			Assert.fail();
		}
		
		context.put(ReliableJdbcEventReader.PASSWORD_CMD_PARAM, "echo password");
		reader.configure(context);
		try {
			reader.readEvent();
			
			Assert.fail();
		} catch (IOException e) {
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
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

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
	public void sameQuery(){

		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.CONNECTION_URL_PARAM, connection_url);
		context.put(ReliableJdbcEventReader.USERNAME_PARAM, "SA");
		context.put(ReliableJdbcEventReader.PASSWORD_PARAM, "");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, " audit_data_table");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

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
			Assert.assertEquals("{\"ID\":2,\"RETURN_CODE\":48,\"NAME\":\"name2\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":1,\"RETURN_CODE\":48,\"NAME\":\"name1\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":3,\"RETURN_CODE\":48,\"NAME\":\"name3\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNull(event);

			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":2,\"RETURN_CODE\":48,\"NAME\":\"name2\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":1,\"RETURN_CODE\":48,\"NAME\":\"name1\"}", new String(event.getBody()));
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
	public void eventsFromDatabaseInBatchFashion(){

		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.CONNECTION_URL_PARAM, connection_url);
		context.put(ReliableJdbcEventReader.USERNAME_PARAM, "SA");
		context.put(ReliableJdbcEventReader.PASSWORD_PARAM, "");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, " audit_data_table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "ID");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "numeric");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

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
			Assert.assertEquals(1, events.size());
			Assert.assertEquals("{\"ID\":3,\"RETURN_CODE\":48,\"NAME\":\"name3\"}",
					new String(events.get(0).getBody()));

			statement = connection.createStatement();
			statement.execute("INSERT INTO audit_data_table VALUES 4, 48, 'name4';");
			statement.execute("INSERT INTO audit_data_table VALUES 5, 48, 'name5';");
			statement.close();
			events = reader.readEvents(1);
			Assert.assertNotNull(events);
			Assert.assertEquals(1, events.size());
			Assert.assertEquals("{\"ID\":3,\"RETURN_CODE\":48,\"NAME\":\"name3\"}",
					new String(events.get(0).getBody()));
			reader.commit();
			events = reader.readEvents(1);
			Assert.assertNotNull(events);
			Assert.assertEquals(1, events.size());
			Assert.assertEquals("{\"ID\":4,\"RETURN_CODE\":48,\"NAME\":\"name4\"}",
					new String(events.get(0).getBody()));
			events = reader.readEvents(1);
			Assert.assertNotNull(events);
			Assert.assertEquals(1, events.size());
			Assert.assertEquals("{\"ID\":5,\"RETURN_CODE\":48,\"NAME\":\"name5\"}",
					new String(events.get(0).getBody()));
			events = reader.readEvents(1);
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
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

		try {
			//Remove commit file
			new File(ReliableJdbcEventReader.COMMITTING_FILE_PATH_DEFAULT).delete();

			Event event = reader.readEvent();
			Assert.assertNull(event);

			Statement statement = connection.createStatement();
			statement.execute("INSERT INTO audit_data_table VALUES 1, 48, 'name1';");
			statement.execute("INSERT INTO audit_data_table VALUES 2, 48, 'name2';");
			statement.execute("INSERT INTO audit_data_table VALUES 3, 48, 'name3';");
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":1,\"RETURN_CODE\":48,\"NAME\":\"name1\"}", new String(event.getBody()));

			reader.configure(context);

			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":1,\"RETURN_CODE\":48,\"NAME\":\"name1\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":2,\"RETURN_CODE\":48,\"NAME\":\"name2\"}", new String(event.getBody()));
			reader.commit();
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":3,\"RETURN_CODE\":48,\"NAME\":\"name3\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNull(event);

			reader.configure(context);

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
	public void assertTypes(){
		String timestamp = "TO_DATE('02/14/2014 08:52:44', 'MM/DD/YYYY HH24:MI:SS')";
		String testTable = "test_types";
		try {
			Statement statement = connection.createStatement();
			statement.execute("DROP TABLE IF EXISTS " + testTable + ";");
			statement.execute("CREATE TABLE " + testTable + " (id INTEGER," +
					"test_smallint SMALLINT," +
					"test_tinyint TINYINT," +
					"test_integer INTEGER," +
					"test_bigint BIGINT," +
					"test_boolean BOOLEAN," +
					"test_numeric_int NUMERIC(7,0)," +
					"test_numeric_float NUMERIC(7,2)," +
					"test_double DOUBLE," +
					"test_float FLOAT," +
					"test_timestamp TIMESTAMP," +
					"test_varchar VARCHAR(20));");
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
			Assert.fail();
		}

		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.CONNECTION_URL_PARAM, connection_url);
		context.put(ReliableJdbcEventReader.USERNAME_PARAM, "SA");
		context.put(ReliableJdbcEventReader.PASSWORD_PARAM, "");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, testTable);
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "ID");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "numeric");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

		try {
			//Remove commit file
			new File(ReliableJdbcEventReader.COMMITTING_FILE_PATH_DEFAULT).delete();

			Event event = reader.readEvent();
			Assert.assertNull(event);

			Statement insertStatement = connection.createStatement();
			String insertSql = "INSERT INTO " + testTable + " VALUES 1, " + // ID
					"2992, " + // Smallint (16 bit)
					"124, " + // Tinyint (8 bit)
					"2147483640, " + // Integer (32 bit)
					"214748364000, " + // Bigint (64 bit)
					"'true', " + // boolean
					"1234, " + // Numeric with 0 precision
					"1234.56, " + // Numeric with 9 precision
					"100.100, " + // Double
					"2245245222.222, " + // Float
					timestamp + ", " + // Timestamp
					"'CERN';"; // String (varchar)
			// Test if all types handle null gracefully (no nasty crash)
			String nullSql = "INSERT INTO " + testTable + " VALUES 2, " + // ID
					"null, " + // Smallint (16 bit)
					"null, " + // Tinyint (8 bit)
					"null, " + // Integer (32 bit)
					"null, " + // Bigint (64 bit)
					"null, " + // boolean
					"null, " + // Numeric with 0 precision
					"null, " + // Numeric with 9 precision
					"null, " + // Double
					"null, " + // Float
					"null, " + // Timestamp
					"null;"; // String (varchar)
			insertStatement.execute(insertSql);
			insertStatement.execute(nullSql);
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":1,\"TEST_SMALLINT\":2992,\"TEST_TINYINT\":124,\"TEST_INTEGER\":2147483640," +
					"\"TEST_BIGINT\":214748364000,\"TEST_BOOLEAN\":true,\"TEST_NUMERIC_INT\":1234.0," +
					"\"TEST_NUMERIC_FLOAT\":1234.56,\"TEST_DOUBLE\":100.1,\"TEST_FLOAT\":2.245245222222E9," +
					"\"TEST_TIMESTAMP\":\"2014-02-14T08:52:44+0100\",\"TEST_VARCHAR\":\"CERN\"}",
					new String(event.getBody()));
			// Now with scale-aware numeric formats:
			context.put(ReliableJdbcEventReader.SCALE_AWARE_NUMERIC_PARAM, Boolean.TRUE.toString());
			reader.configure(context);
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":1,\"TEST_SMALLINT\":2992,\"TEST_TINYINT\":124,\"TEST_INTEGER\":2147483640," +
					"\"TEST_BIGINT\":214748364000,\"TEST_BOOLEAN\":true,\"TEST_NUMERIC_INT\":1234," +
					"\"TEST_NUMERIC_FLOAT\":1234.56,\"TEST_DOUBLE\":100.1,\"TEST_FLOAT\":2.245245222222E9," +
					"\"TEST_TIMESTAMP\":\"2014-02-14T08:52:44+0100\",\"TEST_VARCHAR\":\"CERN\"}",
					new String(event.getBody()));
			// Test if expanding the floats work
			context.put(ReliableJdbcEventReader.EXPAND_BIG_FLOATS_PARAM, Boolean.TRUE.toString());
			reader.configure(context);
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":1,\"TEST_SMALLINT\":2992,\"TEST_TINYINT\":124,\"TEST_INTEGER\":2147483640," +
							"\"TEST_BIGINT\":214748364000,\"TEST_BOOLEAN\":true,\"TEST_NUMERIC_INT\":1234," +
							"\"TEST_NUMERIC_FLOAT\":1234.56,\"TEST_DOUBLE\":100.1,\"TEST_FLOAT\":2245245222.222," +
							"\"TEST_TIMESTAMP\":\"2014-02-14T08:52:44+0100\",\"TEST_VARCHAR\":\"CERN\"}",
					new String(event.getBody()));
			event = reader.readEvent(); // The one with the null values
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":2,\"TEST_SMALLINT\":null,\"TEST_TINYINT\":null,\"TEST_INTEGER\":null," +
					"\"TEST_BIGINT\":null,\"TEST_BOOLEAN\":null,\"TEST_NUMERIC_INT\":null,\"TEST_NUMERIC_FLOAT\":null," +
					"\"TEST_DOUBLE\":null,\"TEST_FLOAT\":null,\"TEST_TIMESTAMP\":null,\"TEST_VARCHAR\":null}",
					new String(event.getBody()));
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
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

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
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

		Assert.assertEquals(timestamp, reader.committed_value);
	}

	@Test
	public void loadFromConfiguredCommittedValue(){

		String timestamp = "2016-02-09 09:34:51.244507 Europe/Zurich";

		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		context.put(ReliableJdbcEventReader.COMMITTED_VALUE_TO_LOAD_PARAM, timestamp);
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

		Assert.assertEquals(timestamp, reader.committed_value);
	}

	@Test
	public void loadFromCommittingFileEvenWhenCommittedValueIsConfigured(){

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
		context.put(ReliableJdbcEventReader.COMMITTED_VALUE_TO_LOAD_PARAM, "wrong");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

		Assert.assertEquals(timestamp, reader.committed_value);
	}

	@Test
	public void readEmptyCommitFile(){

		//It will create the file
		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);
		Assert.assertNull(reader.last_value);

		//It will read an empty file
		reader.configure(context);
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
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

		String result = reader.createQuery(null);
		Assert.assertEquals(result, "new query");

		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PARAM,
				"SELECT * FROM table_name [WHERE column_name > '{$committed_value}'] ORDER BY column_name");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		reader.configure(context);

		result = reader.createQuery(null);
		Assert.assertEquals("SELECT * FROM table_name  ORDER BY column_name", result);


		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PARAM,
				"SELECT * FROM table_name [WHERE column_name > ':committed_value'] ORDER BY column_name");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		reader.configure(context);

		result = reader.createQuery("12345");
		Assert.assertEquals("SELECT * FROM table_name  WHERE column_name > '12345'  ORDER BY column_name", result);


		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PARAM,
				"SELECT * FROM table_name [WHERE column_name1 > ':committed_value'] "
				+ "[AND column_name2 > ':committed_value'] ORDER BY column_name");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		reader.configure(context);

		result = reader.createQuery("12345");
		Assert.assertEquals("SELECT * FROM table_name  WHERE column_name1 > '12345' "
				+ "  AND column_name2 > '12345'  ORDER BY column_name", result);


		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PARAM,
				"SELECT * FROM table_name WHERE column_name1 > ':committed_value' "
				+ "[AND column_name2 > ':committed_value'] ORDER BY column_name");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		reader.configure(context);

		result = reader.createQuery(null);
		Assert.assertEquals("SELECT * FROM table_name WHERE column_name1 > ':committed_value' "
				+ " ORDER BY column_name", result);


		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PARAM,
				"SELECT * FROM table_name [WHERE column_name1 > ':committed_value'] "
				+ "[AND column_name2 > ':committed_value'] ORDER BY column_name");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		reader.configure(context);

		result = reader.createQuery(null);
		Assert.assertEquals("SELECT * FROM table_name   ORDER BY column_name", result);


		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		context.put(ReliableJdbcEventReader.QUERY_PARAM, "new query");
		reader.configure(context);

		result = reader.createQuery("value");
		Assert.assertEquals(result, "new query");


		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name1");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name1");
		reader.configure(context);

		result = reader.createQuery(null);
		Assert.assertEquals(result, "SELECT * FROM table_name1 ORDER BY column_name1");


		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name2");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name2");
		reader.configure(context);

		result = reader.createQuery("2016-02-09 09:34:51.244");
		Assert.assertEquals(result, "SELECT * FROM table_name2 "
				+ "WHERE column_name2 >= TIMESTAMP '2016-02-09 09:34:51.244' "
				+ "ORDER BY column_name2");


		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name2");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name2");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "timestamp");
		reader.configure(context);

		result = reader.createQuery("2016-02-09 09:34:51.244");
		Assert.assertEquals(result, "SELECT * FROM table_name2 "
				+ "WHERE column_name2 >= TIMESTAMP '2016-02-09 09:34:51.244' "
				+ "ORDER BY column_name2");


		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name3");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name3");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "numeric");
		reader.configure(context);

		result = reader.createQuery("244");
		Assert.assertEquals(result, "SELECT * FROM table_name3 "
				+ "WHERE column_name3 >= 244 "
				+ "ORDER BY column_name3");


		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name4");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name4");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "string");
		reader.configure(context);

		result = reader.createQuery("string4");
		Assert.assertEquals(result, "SELECT * FROM table_name4 "
				+ "WHERE column_name4 >= \'string4\' "
				+ "ORDER BY column_name4");


		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name4");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column_name4");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "does_not_exist");

		try{
			reader.configure(context);
			Assert.fail();
		}catch(FlumeException e){
		}

		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, "table_name5");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "string");
		reader.configure(context);

		result = reader.createQuery("string4");
		Assert.assertEquals(result, "SELECT * FROM table_name5");
	}

	@Test
	public void getConfiguredQuery(){

		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PARAM, "parameter_new_query");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

		Assert.assertEquals("parameter_new_query", reader.createQuery(""));

		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PARAM, "parameter_new_query");
		context.put(ReliableJdbcEventReader.QUERY_PATH_PARAM, "path/to/not/existing/file");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		reader.configure(context);

		Assert.assertEquals("parameter_new_query", reader.createQuery(""));

		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PATH_PARAM, "path/to/not/existing/file");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		try{
			reader.configure(context);
			Assert.fail();
		}catch(FlumeException e){}

		context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.QUERY_PATH_PARAM, "src/test/resources/file.query");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		reader.configure(context);

		Assert.assertEquals("query in file\nwith several\nlines", reader.createQuery(""));
	}
	
	@Test
	public void rollBack(){

		Context context = new Context();
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcEventReader.CONNECTION_URL_PARAM, connection_url);
		context.put(ReliableJdbcEventReader.USERNAME_PARAM, "SA");
		context.put(ReliableJdbcEventReader.PASSWORD_PARAM, "");
		context.put(ReliableJdbcEventReader.TABLE_NAME_PARAM, " audit_data_table");
		context.put(ReliableJdbcEventReader.COLUMN_TO_COMMIT_PARAM, "ID");
		context.put(ReliableJdbcEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "numeric");
		ReliableJdbcEventReader reader = new ReliableJdbcEventReader();
		reader.configure(context);

		try {
			Event event = reader.readEvent();
			Assert.assertNull(event);

			Statement statement = connection.createStatement();
			statement.execute("INSERT INTO audit_data_table VALUES 1, 48, 'name1';");
			statement.execute("INSERT INTO audit_data_table VALUES 2, 48, 'name2';");
			statement.execute("INSERT INTO audit_data_table VALUES 3, 48, 'name3';");
			statement.close();

			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":1,\"RETURN_CODE\":48,\"NAME\":\"name1\"}", new String(event.getBody()));
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":2,\"RETURN_CODE\":48,\"NAME\":\"name2\"}", new String(event.getBody()));
			
			reader.commit();
			
			event = reader.readEvent();
			Assert.assertNotNull(event);
			Assert.assertEquals("{\"ID\":3,\"RETURN_CODE\":48,\"NAME\":\"name3\"}", new String(event.getBody()));
			
			reader.rollback();
			
			//NOTICE default query is >= committed_value so we get 2 again
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

	@After
	public void cleanUp(){
		new File(ReliableJdbcEventReader.COMMITTING_FILE_PATH_DEFAULT).delete();

		try {
			connection.close();
		} catch (SQLException e) {}
	}
}