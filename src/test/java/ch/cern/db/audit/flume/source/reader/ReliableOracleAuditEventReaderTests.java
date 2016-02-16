package ch.cern.db.audit.flume.source.reader;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ReliableOracleAuditEventReaderTests {

	@Test
	public void createCommitFile(){
		@SuppressWarnings("resource")
		ReliableOracleAuditEventReader reader = new ReliableOracleAuditEventReader();
		
		String timestamp = "2016-02-09 09:34:51.244507 Europe/Zurich";
		
		reader.last_timestamp = timestamp ;
		try {
			reader.commit();
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
		
		try {
			FileReader in = new FileReader(ReliableOracleAuditEventReader.TIMESTAMP_FILE_PATH);
			char [] in_chars = new char[50];
		    in.read(in_chars);
			in.close();
			
			String timestamp_from_file = new String(in_chars).trim();
			
			Assert.assertEquals(timestamp, reader.last_commited_timestamp);
			Assert.assertEquals(timestamp, timestamp_from_file);
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void readCommitFile(){
		
		String timestamp = "2016-02-09 09:34:51.244507 Europe/Zurich";
		
		try {
			FileWriter out = new FileWriter(ReliableOracleAuditEventReader.TIMESTAMP_FILE_PATH, false);
			out.write(timestamp);
			out.close();
		} catch (IOException e) {
			Assert.fail();
		}
		
		@SuppressWarnings("resource")
		ReliableOracleAuditEventReader reader = new ReliableOracleAuditEventReader();
		
		Assert.assertEquals(timestamp, reader.last_commited_timestamp);
	}
	
	@Test
	public void readEmptyCommitFile(){
		
		//It will create the file
		@SuppressWarnings("resource")
		ReliableOracleAuditEventReader reader = new ReliableOracleAuditEventReader();
		Assert.assertNull(reader.last_commited_timestamp);
		
		//It will read an empty file
		reader = new ReliableOracleAuditEventReader();
		Assert.assertNull(reader.last_commited_timestamp);
		
		String timestamp = "2016-02-09 09:34:51.244507 Europe/Zurich";
		reader.last_timestamp = timestamp;
		try {
			reader.commit();
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
		
		try {
			FileReader in = new FileReader(ReliableOracleAuditEventReader.TIMESTAMP_FILE_PATH);
			char [] in_chars = new char[50];
		    in.read(in_chars);
			in.close();
			
			String timestamp_from_file = new String(in_chars).trim();
			
			Assert.assertEquals(timestamp, reader.last_commited_timestamp);
			Assert.assertEquals(timestamp, timestamp_from_file);
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
	}

	@After
	public void cleanUp(){
		new File(ReliableOracleAuditEventReader.TIMESTAMP_FILE_PATH).delete();
	}
}