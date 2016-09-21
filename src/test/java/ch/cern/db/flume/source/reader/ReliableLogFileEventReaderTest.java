/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.source.reader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.flume.source.reader.log.DefaultLogEventParser;

public class ReliableLogFileEventReaderTest {
	
	private File temp_log_file = new File("src/test/resources/sample-logs/listener-tmp.log");

	@Test
	public void readLogEvents(){
		ReliableLogFileEventReader reader = new ReliableLogFileEventReader();
		Context context = new Context();
		context.put(ReliableLogFileEventReader.LOG_FILE_PATH_PARAM, 
				"src/test/resources/sample-logs/listener.log");
		context.put(ReliableLogFileEventReader.DATAFORMAT_PARAM, "dd-MMM-yyy HH:mm:ss");
		reader.configure(context);
		
		int i = 0;
		Event event;
		while((event = reader.readEvent()) != null){	
			i++;
			
			Assert.assertNotNull(event);
		}
		
		Assert.assertEquals(13, i);
	}
	
	@Test
	public void linesAddedToFile() throws IOException{
		File file = new File("src/test/resources/sample-logs/listener-tmp.log");
		
		ReliableLogFileEventReader reader = new ReliableLogFileEventReader();
		Context context = new Context();
		context.put(ReliableLogFileEventReader.LOG_FILE_PATH_PARAM, file.getAbsolutePath());
		context.put(ReliableLogFileEventReader.DATAFORMAT_PARAM, "dd-MMM-yyy HH:mm:ss");
		reader.configure(context);
		
		//File does not exist
		int counter = 0;
		for (int tries = 0; tries < 5; tries++)
			if(reader.readEvent() != null)
				counter++;
		Assert.assertEquals(0, counter);
		
		//Empty file exists
		file.createNewFile();
		
		counter = 0;
		for (int tries = 0; tries < 5; tries++)
			if(reader.readEvent() != null)
				counter++;
		Assert.assertEquals(0, counter);
				
		//Insert a few lines from sample log file
		File sampleFile = new File("src/test/resources/sample-logs/listener.log");
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sampleFile)));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
		
		for (int line = 0; line < 4; line++)
			bw.write(br.readLine() + "\n");
		bw.flush();
		
		counter = 0;
		for (int tries = 0; tries < 15; tries++)
			if(reader.readEvent() != null)
				counter++;
		Assert.assertEquals(2, counter);
		
		//Insert a few more lines from sample log file
		for (int line = 0; line < 12; line++)
			bw.write(br.readLine() + "\n");
		bw.flush();
			
		counter = 0;
		for (int tries = 0; tries < 15; tries++)
			if(reader.readEvent() != null)
				counter++;
		Assert.assertEquals(8, counter);
		
		//Insert the rest from sample log file
		String line = null;
		while((line = br.readLine()) != null)
			bw.write(line + "\n");
		bw.flush();
		
		counter = 0;
		for (int tries = 0; tries < 15; tries++)
			if(reader.readEvent() != null)
				counter++;
		Assert.assertEquals(3, counter);
		
		//Clean
		try {
			br.close();
		} catch (IOException e) {}
		try {
			bw.close();
		} catch (IOException e) {}
	}
	
	@Test
	public void fileRolledOut() throws IOException{

		ReliableLogFileEventReader reader = new ReliableLogFileEventReader();
		Context context = new Context();
		context.put(ReliableLogFileEventReader.LOG_FILE_PATH_PARAM, temp_log_file.getAbsolutePath());
		context.put(ReliableLogFileEventReader.DATAFORMAT_PARAM, "dd-MMM-yyy HH:mm:ss");
		reader.configure(context);
		
		//File does not exist
		int counter = 0;
		for (int tries = 0; tries < 5; tries++)
			if(reader.readEvent() != null)
				counter++;
		Assert.assertEquals(0, counter);
		
		//Empty file exists
		temp_log_file.createNewFile();
		
		counter = 0;
		for (int tries = 0; tries < 5; tries++)
			if(reader.readEvent() != null)
				counter++;
		Assert.assertEquals(0, counter);
				
		//Insert a few lines from sample log file
		File sampleFile = new File("src/test/resources/sample-logs/listener.log");
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sampleFile)));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(temp_log_file)));
		
		for (int line = 0; line < 4; line++)
			bw.write(br.readLine() + "\n");
		bw.flush();
		
		counter = 0;
		for (int tries = 0; tries < 15; tries++)
			if(reader.readEvent() != null)
				counter++;
		Assert.assertEquals(2, counter);
		
		//Insert a few more lines from sample log file
		for (int line = 0; line < 12; line++)
			bw.write(br.readLine() + "\n");
		bw.flush();
			
		counter = 0;
		for (int tries = 0; tries < 15; tries++)
			if(reader.readEvent() != null)
				counter++;
		Assert.assertEquals(8, counter);
		
		//Roll out file
		temp_log_file.delete();
		temp_log_file.createNewFile();
		
		//Try to read empty file
		counter = 0;
		for (int tries = 0; tries < 5; tries++)
			if(reader.readEvent() != null)
				counter++;
		Assert.assertEquals(0, counter);
		
		//Insert the rest from sample log file
		bw.close();
		bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(temp_log_file)));
		String line = null;
		while((line = br.readLine()) != null)
			bw.write(line + "\n");
		bw.flush();
		
		counter = 0;
		for (int tries = 0; tries < 15; tries++)
			if(reader.readEvent() != null)
				counter++;
		Assert.assertEquals(3, counter);
		
		//Close buffers
		try {
			br.close();
		} catch (IOException e) {}
		try {
			bw.close();
		} catch (IOException e) {}
	}
	
	@Test
	public void commitAndReload() throws IOException{
		ReliableLogFileEventReader reader = new ReliableLogFileEventReader();
		Context context = new Context();
		context.put(ReliableLogFileEventReader.LOG_FILE_PATH_PARAM, "src/test/resources/sample-logs/listener.log");
		context.put(ReliableLogFileEventReader.DATAFORMAT_PARAM, "dd-MMM-yyy HH:mm:ss");
		reader.configure(context);
				
		//Read a few events
		Assert.assertEquals("2016-07-29T15:17:34+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:17:38+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:18:45+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:19:55+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:19:56+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		
		//Commit
		reader.commit();
		
		//Confirm content of committing file
		BufferedReader committing_file_br = new BufferedReader(new InputStreamReader(
				new FileInputStream(new File(ReliableLogFileEventReader.COMMITTING_FILE_PATH_DEFAULT))));
		Assert.assertEquals("2016-07-29T15:19:56+0200", committing_file_br.readLine());
		committing_file_br.close();
		
		//Reload reader
		reader = new ReliableLogFileEventReader();
		reader.configure(context);
		
		//Read a few more events
		//SAME AS LAST EVENT SINCE IS GREATHER OR EQUAL (THAT'S OK, DUPLICATED EVENTS INTERCEPTOR WILL DROP THE EVENT)
		Assert.assertEquals("2016-07-29T15:19:56+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:20:08+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:20:10+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		
		//Confirm content of committing file
		committing_file_br = new BufferedReader(new InputStreamReader(
				new FileInputStream(new File(ReliableLogFileEventReader.COMMITTING_FILE_PATH_DEFAULT))));
		Assert.assertEquals("2016-07-29T15:19:56+0200", committing_file_br.readLine());
		committing_file_br.close();
		
		//Reload reader without committing
		reader = new ReliableLogFileEventReader();
		reader.configure(context);
		
		//Read again same batch (because was not committed)
		Assert.assertEquals("2016-07-29T15:19:56+0200", reader.readEvent().getHeaders().get(DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:20:08+0200", reader.readEvent().getHeaders().get(DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:20:10+0200", reader.readEvent().getHeaders().get(DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		
		//Continue reading
		//Special case: two events with same timestamp
		Assert.assertEquals("2016-07-29T15:20:17+0200", reader.readEvent().getHeaders().get(DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:20:17+0200", reader.readEvent().getHeaders().get(DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		
		//Commit and reload reader
		reader.commit();
		reader = new ReliableLogFileEventReader();
		reader.configure(context);
		
		//Both events with the same timestamo will be reload
		Assert.assertEquals("2016-07-29T15:20:17+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:20:17+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:20:57+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:21:06+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:22:13+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:22:29+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		
		//End of file
		Assert.assertNull(reader.readEvent());
	}
	
	@Test
	public void tryToLoadOlderEvents() throws IOException{
		File logFile = new File("src/test/resources/sample-logs/listener-tmp.log");
		
		ReliableLogFileEventReader reader = new ReliableLogFileEventReader();
		Context context = new Context();
		context.put(ReliableLogFileEventReader.LOG_FILE_PATH_PARAM, logFile.getAbsolutePath());
		context.put(ReliableLogFileEventReader.DATAFORMAT_PARAM, "dd-MMM-yyy HH:mm:ss");
		reader.configure(context);
				
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile)));
		
		//Write events
		bw.write("29-Jul-2016 15:20:01 * event text\n");
		bw.write("29-Jul-2016 15:20:02 * event text\n");
		bw.write("29-Jul-2016 15:20:00 * event text\n");
		bw.write("29-Jul-2016 15:20:00 * event text\n");
		bw.write("29-Jul-2016 15:20:03 * event text\n");
		bw.flush();
		
		Assert.assertEquals("2016-07-29T15:20:01+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:20:02+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:20:03+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		
		bw.close();
	}

	@Test
	public void rollBack() throws IOException{

		ReliableLogFileEventReader reader = new ReliableLogFileEventReader();
		Context context = new Context();
		context.put(ReliableLogFileEventReader.LOG_FILE_PATH_PARAM, "src/test/resources/sample-logs/listener.log");
		context.put(ReliableLogFileEventReader.DATAFORMAT_PARAM, "dd-MMM-yyy HH:mm:ss");
		reader.configure(context);

		//Read a few events
		Assert.assertEquals("2016-07-29T15:17:34+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:17:38+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		
		reader.commit();
		
		Assert.assertEquals("2016-07-29T15:18:45+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:19:55+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		
		reader.rollback();
		
		// >= timestamp condition is used, so we get 2016-07-29T15:17:38 again
		Assert.assertEquals("2016-07-29T15:17:38+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:18:45+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
		Assert.assertEquals("2016-07-29T15:19:55+0200", reader.readEvent().getHeaders().get(
				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
	}
	
	@After
	public void cleanUp(){
		new File(ReliableLogFileEventReader.COMMITTING_FILE_PATH_DEFAULT).delete();
		temp_log_file.delete();
	}

}
