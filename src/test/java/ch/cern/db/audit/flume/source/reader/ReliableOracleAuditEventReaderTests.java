package ch.cern.db.audit.flume.source.reader;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ReliableOracleAuditEventReaderTests {

	@Test
	public void createCommitFile(){
		@SuppressWarnings("resource")
		ReliableOracleAuditEventReader reader = new ReliableOracleAuditEventReader();
		
		String timestamp = "2016-02-09 09:34:51.244507 Europe/Zurich";
		
		reader.last_value = timestamp ;
		try {
			reader.commit();
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
		
		try {
			FileReader in = new FileReader(ReliableOracleAuditEventReader.COMMITTING_FILE_PATH_PARAM);
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
			FileWriter out = new FileWriter(ReliableOracleAuditEventReader.COMMITTING_FILE_PATH_PARAM, false);
			out.write(timestamp);
			out.close();
		} catch (IOException e) {
			Assert.fail();
		}
		
		@SuppressWarnings("resource")
		ReliableOracleAuditEventReader reader = new ReliableOracleAuditEventReader();
		
		Assert.assertEquals(timestamp, reader.committed_value);
	}
	
	@Test
	public void readEmptyCommitFile(){
		
		//It will create the file
		@SuppressWarnings("resource")
		ReliableOracleAuditEventReader reader = new ReliableOracleAuditEventReader();
		Assert.assertNull(reader.committed_value);
		
		//It will read an empty file
		reader = new ReliableOracleAuditEventReader();
		Assert.assertNull(reader.committed_value);
		
		String timestamp = "2016-02-09 09:34:51.244507 Europe/Zurich";
		reader.last_value = timestamp;
		try {
			reader.commit();
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
		
		try {
			FileReader in = new FileReader(ReliableOracleAuditEventReader.COMMITTING_FILE_PATH_PARAM);
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
		
		ReliableOracleAuditEventReader reader = Mockito.spy(ReliableOracleAuditEventReader.class);
		
		String result = reader.createQuery("new query", null, null, null, null);
		Assert.assertEquals(result, "new query");
		
		result = reader.createQuery("new query", "a", "b", 0, "c");
		Assert.assertEquals(result, "new query");
		
		result = reader.createQuery(null, "table_name", "column_name", java.sql.Types.TIMESTAMP, null);
		Assert.assertEquals(result, "SELECT * FROM table_name "
				+ "ORDER BY column_name");
		
		result = reader.createQuery(null, "table_name", "column_name", java.sql.Types.TIMESTAMP, "2016-02-09 09:34:51.244");
		Assert.assertEquals(result, "SELECT * FROM table_name "
				+ "WHERE column_name > TIMESTAMP '2016-02-09 09:34:51.244' "
				+ "ORDER BY column_name");
		
		result = reader.createQuery(null, "table_name", "column_name", java.sql.Types.NUMERIC, "244");
		Assert.assertEquals(result, "SELECT * FROM table_name "
				+ "WHERE column_name > 244 "
				+ "ORDER BY column_name");
		
		result = reader.createQuery(null, "table_name", "column_name", java.sql.Types.CHAR, "string");
		Assert.assertEquals(result, "SELECT * FROM table_name "
				+ "WHERE column_name > \'string\' "
				+ "ORDER BY column_name");
	}

	@After
	public void cleanUp(){
		new File(ReliableOracleAuditEventReader.COMMITTING_FILE_PATH_PARAM).delete();
	}
}