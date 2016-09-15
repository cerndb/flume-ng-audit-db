/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.log;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class LogFileTest {

	@Test
	public void readLogEvents(){
		LogFile logFile = new LogFile("src/test/resources/sample-logs/listener.log");
		logFile.setDateFormat(new SimpleDateFormat("dd-MMM-yyy HH:mm:ss"));
		
		int i = 0;
		LogEvent event;
		while((event = logFile.getNextEvent()) != null){	
			i++;
			
			Assert.assertNotNull(event);
		}
		
		Assert.assertEquals(13, i);
	}

	@Test
	public void eventsCanContainSeveralLines(){
		LogFile logFile = new LogFile("src/test/resources/sample-logs/listener.log");
		logFile.setDateFormat(new SimpleDateFormat("dd-MMM-yyy HH:mm:ss"));
		
		int severalLinesEvents_counter = 0;
		int oneLineEvents_counter = 0;
		
		LogEvent event;
		while((event = logFile.getNextEvent()) != null){	
			if(event.getText().contains("\n"))
				severalLinesEvents_counter++;
			else
				oneLineEvents_counter++;
		}
		
		Assert.assertEquals(7, severalLinesEvents_counter);
		Assert.assertEquals(6, oneLineEvents_counter);
	}
	
	@Test
	public void eventsCannotContainSeveralLines(){
		LogFile logFile = new LogFile("src/test/resources/sample-logs/listener.log");
		logFile.setDateFormat(new SimpleDateFormat("dd-MMM-yyy HH:mm:ss"));
		logFile.setEventsCanContainSeveralLines(false);
		
		int severalLinesEvents_counter = 0;
		int oneLineEvents_counter = 0;
		
		LogEvent event;
		while((event = logFile.getNextEvent()) != null){	
			if(event.getText().contains("\n"))
				severalLinesEvents_counter++;
			else
				oneLineEvents_counter++;
		}
		
		Assert.assertEquals(0, severalLinesEvents_counter);
		Assert.assertEquals(13, oneLineEvents_counter);
	}
	
	@Test
	public void linesAddedToFile() throws IOException{
		File file = new File("src/test/resources/sample-logs/listener-tmp.log");
		
		LogFile logFile = new LogFile(file.getAbsolutePath());
		logFile.setDateFormat(new SimpleDateFormat("dd-MMM-yyy HH:mm:ss"));
		
		//File does not exist
		int counter = 0;
		for (int tries = 0; tries < 5; tries++)
			if(logFile.getNextEvent() != null)
				counter++;
		Assert.assertEquals(0, counter);
		
		//Empty file exists
		file.createNewFile();
		
		counter = 0;
		for (int tries = 0; tries < 5; tries++)
			if(logFile.getNextEvent() != null)
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
			if(logFile.getNextEvent() != null)
				counter++;
		Assert.assertEquals(2, counter);
		
		//Insert a few more lines from sample log file
		for (int line = 0; line < 12; line++)
			bw.write(br.readLine() + "\n");
		bw.flush();
			
		counter = 0;
		for (int tries = 0; tries < 15; tries++)
			if(logFile.getNextEvent() != null)
				counter++;
		Assert.assertEquals(8, counter);
		
		//Insert the rest from sample log file
		String line = null;
		while((line = br.readLine()) != null)
			bw.write(line + "\n");
		bw.flush();
		
		counter = 0;
		for (int tries = 0; tries < 15; tries++)
			if(logFile.getNextEvent() != null)
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
		File file = new File("src/test/resources/sample-logs/listener-tmp.log");
		
		LogFile logFile = new LogFile(file.getAbsolutePath());
		logFile.setDateFormat(new SimpleDateFormat("dd-MMM-yyy HH:mm:ss"));
		
		//File does not exist
		int counter = 0;
		for (int tries = 0; tries < 5; tries++)
			if(logFile.getNextEvent() != null)
				counter++;
		Assert.assertEquals(0, counter);
		
		//Empty file exists
		file.createNewFile();
		
		counter = 0;
		for (int tries = 0; tries < 5; tries++)
			if(logFile.getNextEvent() != null)
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
			if(logFile.getNextEvent() != null)
				counter++;
		Assert.assertEquals(2, counter);
		
		//Insert a few more lines from sample log file
		for (int line = 0; line < 12; line++)
			bw.write(br.readLine() + "\n");
		bw.flush();
			
		counter = 0;
		for (int tries = 0; tries < 15; tries++)
			if(logFile.getNextEvent() != null)
				counter++;
		Assert.assertEquals(8, counter);
		
		//Roll out file
		file.delete();
		file.createNewFile();
		
		//Try to read empty file
		counter = 0;
		for (int tries = 0; tries < 5; tries++)
			if(logFile.getNextEvent() != null)
				counter++;
		Assert.assertEquals(0, counter);
		
		//Insert the rest from sample log file
		bw.close();
		bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
		String line = null;
		while((line = br.readLine()) != null)
			bw.write(line + "\n");
		bw.flush();
		
		counter = 0;
		for (int tries = 0; tries < 15; tries++)
			if(logFile.getNextEvent() != null)
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
	
	@After
	public void clean(){
		new File("src/test/resources/sample-logs/listener-tmp.log").delete();
	}
	
}
