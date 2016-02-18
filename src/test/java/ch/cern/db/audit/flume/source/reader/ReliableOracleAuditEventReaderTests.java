package ch.cern.db.audit.flume.source.reader;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.flume.Context;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.audit.flume.source.reader.ReliableOracleAuditEventReader.ColumnType;

public class ReliableOracleAuditEventReaderTests {

	@Test
	public void createCommitFile(){
		Context context = new Context();
		context.put(ReliableOracleAuditEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		@SuppressWarnings("resource")
		ReliableOracleAuditEventReader reader = new ReliableOracleAuditEventReader(context);
		
		String timestamp = "2016-02-09 09:34:51.244507 Europe/Zurich";
		
		reader.last_value = timestamp ;
		try {
			reader.commit();
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
		
		try {
			FileReader in = new FileReader(ReliableOracleAuditEventReader.COMMITTING_FILE_PATH_DEFAULT);
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
			FileWriter out = new FileWriter(ReliableOracleAuditEventReader.COMMITTING_FILE_PATH_DEFAULT, false);
			out.write(timestamp);
			out.close();
		} catch (IOException e) {
			Assert.fail();
		}
		
		Context context = new Context();
		context.put(ReliableOracleAuditEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		@SuppressWarnings("resource")
		ReliableOracleAuditEventReader reader = new ReliableOracleAuditEventReader(context);
		
		Assert.assertEquals(timestamp, reader.committed_value);
	}
	
	@Test
	public void readEmptyCommitFile(){
		
		//It will create the file
		Context context = new Context();
		context.put(ReliableOracleAuditEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		@SuppressWarnings("resource")
		ReliableOracleAuditEventReader reader = new ReliableOracleAuditEventReader(context);
		Assert.assertNull(reader.committed_value);
		
		//It will read an empty file
		reader = new ReliableOracleAuditEventReader(context);
		Assert.assertNull(reader.committed_value);
		
		String timestamp = "2016-02-09 09:34:51.244507 Europe/Zurich";
		reader.last_value = timestamp;
		try {
			reader.commit();
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
		
		try {
			FileReader in = new FileReader(ReliableOracleAuditEventReader.COMMITTING_FILE_PATH_DEFAULT);
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
		context.put(ReliableOracleAuditEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		@SuppressWarnings("resource")
		ReliableOracleAuditEventReader reader = new ReliableOracleAuditEventReader(context);
		
		String result = reader.createQuery("new query", null, null, null, null);
		Assert.assertEquals(result, "new query");
		
		result = reader.createQuery("new query", "a", "b", ColumnType.NUMERIC, "c");
		Assert.assertEquals(result, "new query");
		
		result = reader.createQuery(null, "table_name", "column_name", ColumnType.NUMERIC, null);
		Assert.assertEquals(result, "SELECT * FROM table_name "
				+ "ORDER BY column_name");
		
		result = reader.createQuery(null, "table_name", "column_name", ColumnType.TIMESTAMP, "2016-02-09 09:34:51.244");
		Assert.assertEquals(result, "SELECT * FROM table_name "
				+ "WHERE column_name > TIMESTAMP '2016-02-09 09:34:51.244' "
				+ "ORDER BY column_name");
		
		result = reader.createQuery(null, "table_name", "column_name", ColumnType.NUMERIC, "244");
		Assert.assertEquals(result, "SELECT * FROM table_name "
				+ "WHERE column_name > 244 "
				+ "ORDER BY column_name");
		
		result = reader.createQuery(null, "table_name", "column_name", ColumnType.STRING, "string");
		Assert.assertEquals(result, "SELECT * FROM table_name "
				+ "WHERE column_name > \'string\' "
				+ "ORDER BY column_name");
	}

	@After
	public void cleanUp(){
		new File(ReliableOracleAuditEventReader.COMMITTING_FILE_PATH_DEFAULT).delete();
	}
}