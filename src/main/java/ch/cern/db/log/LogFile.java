package ch.cern.db.log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.db.utils.SUtils;

public class LogFile extends File {

	private static final long serialVersionUID = 1676485524518325649L;
	
	private static final Logger LOG = LoggerFactory.getLogger(LogFile.class);
	
	private BufferedReader bufferReader;

	private String last_line;

	public static final String DATAFORMAT_DEFAULT = "yyyy-MM-dd'T'HH:mm:ssZ";
	private DateFormat dateFormat;
	
	private boolean eventsCanContainSeveralLines = true;
	
	private long offset;
	
	public LogFile(String pathname) {
		super(pathname);
		
		dateFormat = new SimpleDateFormat(DATAFORMAT_DEFAULT);
	}

	public void open() {
		
		if(bufferReader != null)
			return;
		
		if(!exists()){
			LOG.warn(this + " does not exist");
			
			return;
		}
			
		if(!canRead()){
			LOG.warn(this + " is not readable");
			return;
		}
			
		try {
			bufferReader = new BufferedReader(new java.io.FileReader(this));
			
			offset = 0;
			
			LOG.debug(this + " has been openned");
		} catch (FileNotFoundException e) {
			LOG.warn(this + " does not exist", e);
			
			close();
		}
		
	}

	public void close() {
		LOG.debug(this + " has been closed");
		
		if(bufferReader != null){
			try {
				bufferReader.close();
			} catch (IOException e1) {}
		}
		
		bufferReader = null;
		offset = -1;
		last_line = null;
	}

	public LogEvent getNextEvent() {
		if(offset -1 > length()){
			close();
			
			LOG.info(this + " has been rolled out (number of bytes read is greather than file size)");
		}
		
		open();
		
    	//Read line by line till next event or end of file
    	try {
			last_line = last_line != null ? last_line : readLine();

	    	while(last_line != null){
	    		if(LogEvent.isNewEvent(dateFormat, last_line)){
	    			return createEvent();
	        	}else{
	        		LOG.warn("Skipping line from log file: " + last_line);
	        	}
	    		
	    		last_line = readLine();
	    	}
	    	
    	} catch (IOException e) {
			LOG.error("Error reading from " + this, e);
			
			close();
		}

		return null;		
	}

	private String readLine() throws IOException {
		if(bufferReader == null)
			return null;
		
		String line = bufferReader.readLine();
		
		if(line == null)
			return null;
		
		offset += line.length() + SUtils.EOL.length();
				
		return line;
	}

	private LogEvent createEvent() throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append(last_line);
		
		last_line = readLine();
		while(eventsCanContainSeveralLines 
				&& last_line != null 
				&& !LogEvent.isNewEvent(dateFormat, last_line)){
			sb.append("\n" + last_line);
			
			last_line = readLine();
		}
		
		try {
			return new LogEvent(dateFormat, sb.toString());
		} catch (ParseException e) {
			LOG.error("Error creating LogEvent because date format: " + sb.toString());
			
			return null;
		}
	}

	public void setDateFormat(DateFormat dateFormat) {
		this.dateFormat = dateFormat;
	}

	public boolean isEventsCanContainSeveralLines() {
		return eventsCanContainSeveralLines;
	}

	public void setEventsCanContainSeveralLines(boolean eventsCanContainSeveralLines) {
		this.eventsCanContainSeveralLines = eventsCanContainSeveralLines;
	}
	
}
