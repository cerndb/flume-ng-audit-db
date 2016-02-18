package ch.cern.db.audit.flume.source.reader;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

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
	@SuppressWarnings("resource")
	public void queryCreation(){
		
		Context context = new Context();
		context.put(ReliableOracleAuditEventReader.QUERY_PARAM, "new query");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		ReliableOracleAuditEventReader reader = new ReliableOracleAuditEventReader(context);
		
		String result = reader.createQuery(null);
		Assert.assertEquals(result, "new query");
		
		context = new Context();
		context.put(ReliableOracleAuditEventReader.QUERY_PARAM, 
				"SELECT * FROM table_name [WHERE column_name > '{$committed_value}'] ORDER BY column_name");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		reader = new ReliableOracleAuditEventReader(context);
		
		result = reader.createQuery(null);
		Assert.assertEquals(result, "SELECT * FROM table_name  ORDER BY column_name");
		
		
		context = new Context();
		context.put(ReliableOracleAuditEventReader.QUERY_PARAM, 
				"SELECT * FROM table_name [WHERE column_name > '{$committed_value}'] ORDER BY column_name");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		reader = new ReliableOracleAuditEventReader(context);
		
		result = reader.createQuery("12345");
		Assert.assertEquals(result, "SELECT * FROM table_name  WHERE column_name > '12345'  ORDER BY column_name");
		
		
		context = new Context();
		context.put(ReliableOracleAuditEventReader.TABLE_NAME_PARAM, "table");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column");
		context.put(ReliableOracleAuditEventReader.QUERY_PARAM, "new query");
		reader = new ReliableOracleAuditEventReader(context);
		
		result = reader.createQuery("value");
		Assert.assertEquals(result, "new query");
		
		
		context = new Context();
		context.put(ReliableOracleAuditEventReader.TABLE_NAME_PARAM, "table_name1");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column_name1");
		reader = new ReliableOracleAuditEventReader(context);
		
		result = reader.createQuery(null);
		Assert.assertEquals(result, "SELECT * FROM table_name1 ORDER BY column_name1");
		
		
		context = new Context();
		context.put(ReliableOracleAuditEventReader.TABLE_NAME_PARAM, "table_name2");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column_name2");
		reader = new ReliableOracleAuditEventReader(context);
		
		result = reader.createQuery("2016-02-09 09:34:51.244");
		Assert.assertEquals(result, "SELECT * FROM table_name2 "
				+ "WHERE column_name2 > TIMESTAMP '2016-02-09 09:34:51.244' "
				+ "ORDER BY column_name2");
		
		
		context = new Context();
		context.put(ReliableOracleAuditEventReader.TABLE_NAME_PARAM, "table_name2");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column_name2");
		context.put(ReliableOracleAuditEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "timestamp");
		reader = new ReliableOracleAuditEventReader(context);
		
		result = reader.createQuery("2016-02-09 09:34:51.244");
		Assert.assertEquals(result, "SELECT * FROM table_name2 "
				+ "WHERE column_name2 > TIMESTAMP '2016-02-09 09:34:51.244' "
				+ "ORDER BY column_name2");
		
		
		context = new Context();
		context.put(ReliableOracleAuditEventReader.TABLE_NAME_PARAM, "table_name3");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column_name3");
		context.put(ReliableOracleAuditEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "numeric");
		reader = new ReliableOracleAuditEventReader(context);
		
		result = reader.createQuery("244");
		Assert.assertEquals(result, "SELECT * FROM table_name3 "
				+ "WHERE column_name3 > 244 "
				+ "ORDER BY column_name3");
		
		
		context = new Context();
		context.put(ReliableOracleAuditEventReader.TABLE_NAME_PARAM, "table_name4");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column_name4");
		context.put(ReliableOracleAuditEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "string");
		reader = new ReliableOracleAuditEventReader(context);
		
		result = reader.createQuery("string4");
		Assert.assertEquals(result, "SELECT * FROM table_name4 "
				+ "WHERE column_name4 > \'string4\' "
				+ "ORDER BY column_name4");
		
		
		context = new Context();
		context.put(ReliableOracleAuditEventReader.TABLE_NAME_PARAM, "table_name4");
		context.put(ReliableOracleAuditEventReader.COLUMN_TO_COMMIT_PARAM, "column_name4");
		context.put(ReliableOracleAuditEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "does_not_exist");
		
		try{
			reader = new ReliableOracleAuditEventReader(context);
			Assert.fail();
		}catch(FlumeException e){
			System.out.println(e.getMessage());
		}
	}

	@After
	public void cleanUp(){
		new File(ReliableOracleAuditEventReader.COMMITTING_FILE_PATH_DEFAULT).delete();
	}
}