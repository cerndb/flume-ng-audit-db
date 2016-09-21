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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.db.flume.source.reader.log.LogEventParser;
import ch.cern.db.flume.source.reader.log.LogEventParser.Builder;
import ch.cern.db.log.LogEvent;
import ch.cern.db.log.LogFile;

public class ReliableLogFileEventReader implements Configurable{

	private static final Logger LOG = LoggerFactory.getLogger(ReliableLogFileEventReader.class);
	
	enum State{INITIALIZED, CONFIGURED};
	private State state;

	public static final String LOG_FILE_PATH_PARAM = "reader.path";
	private LogFile logFile;
	
	public static final String DATAFORMAT_PARAM = "reader.dateFormat";
	
	public static final String COMMITTING_FILE_PATH_DEFAULT = "committed_date.backup";
	public static final String COMMITTING_FILE_PATH_PARAM = "reader.committingFile";
	private String committing_file_path = COMMITTING_FILE_PATH_DEFAULT;
	private File committing_file = null;
	
	public static final String COMMITTED_VALUE_TO_LOAD_PARAM = "reader.committtedDate";
	private Date committed_date_to_load = null;
	
	public static final boolean LOG_EVENTS_WITH_SEVERAL_LINES_DEFAULT = true;
	public static final String LOG_EVENTS_WITH_SEVERAL_LINES_PARAM = "reader.severalLines";

	private SimpleDateFormat internalDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	
	public static final String PARSER_DEFAULT = "ch.cern.db.flume.source.reader.log.DefaultLogEventParser$Builder";
	public static final String PARSER_PARAM = "reader.parser";
	private LogEventParser parser;
	
	protected Date last_date = null;

	private Date last_committed_date;

	public ReliableLogFileEventReader() {
		initialize();
	}

	private void initialize() {
		last_date = null;
		
		state = State.INITIALIZED;
	}
	
	@Override
	public void configure(Context context) {
		initialize();
		
		String logFilePath = context.getString(LOG_FILE_PATH_PARAM);
		if(logFilePath == null)
			throw new ConfigurationException("Path to log file needs to be specified with " + LOG_FILE_PATH_PARAM);
		if(logFile != null)
			logFile.close();
		logFile = new LogFile(logFilePath);
		logFile.open();
		
		logFile.setDateFormat(new SimpleDateFormat(context.getString(DATAFORMAT_PARAM, LogFile.DATAFORMAT_DEFAULT)));
		logFile.setEventsCanContainSeveralLines(context.getBoolean(LOG_EVENTS_WITH_SEVERAL_LINES_PARAM, 
																	LOG_EVENTS_WITH_SEVERAL_LINES_DEFAULT));
		
		parser = createParser(context);
		
		committing_file_path = context.getString(COMMITTING_FILE_PATH_PARAM, COMMITTING_FILE_PATH_DEFAULT);
		committing_file = new File(committing_file_path);
		
		loadLastCommittedDateFromFile();
		if(last_date == null){
			try {
				String committed_date_to_load_string = context.getString(COMMITTED_VALUE_TO_LOAD_PARAM);
				
				if(committed_date_to_load_string != null)
					committed_date_to_load = internalDateFormat.parse(committed_date_to_load_string);
			} catch (ParseException e) {
				throw new ConfigurationException("Configured date with "
						+ COMMITTED_VALUE_TO_LOAD_PARAM
						+ " param does not meet the required date format ("
						+ internalDateFormat
						+ ")", e);
			}
			
			last_date = committed_date_to_load;
		}
		
		state = State.CONFIGURED;
	}

	private LogEventParser createParser(Context context) {
		try {
			String parserClass = context.getString(PARSER_PARAM, PARSER_DEFAULT);

			@SuppressWarnings("unchecked")
			Class<? extends Builder> clazz = (Class<? extends Builder>) Class.forName(parserClass);

			return clazz.newInstance().build(context);
		} catch (ClassNotFoundException e) {
			LOG.error("Builder class not found. Exception follows.", e);
			throw new FlumeException("LogEventParser.Builder not found.", e);
		} catch (InstantiationException e) {
			LOG.error("Could not instantiate Builder. Exception follows.", e);
			throw new FlumeException("LogEventParser.Builder not constructable.", e);
		} catch (IllegalAccessException e) {
			LOG.error("Unable to access Builder. Exception follows.", e);
			throw new FlumeException("Unable to access LogEventParser.Builder.", e);
		}
	}

	private void loadLastCommittedDateFromFile() {
		if(state != State.INITIALIZED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not initialized");

		try {
			if(committing_file.exists()){
				FileReader in = new FileReader(committing_file);
				char [] in_chars = new char[(int) committing_file.length()];
			    in.read(in_chars);
				in.close();
				String date_from_file = new String(in_chars).trim();

				if(date_from_file.length() > 0){
					try {
						last_date = internalDateFormat.parse(date_from_file);
					} catch (ParseException e) {
						throw new FlumeException("Date from committing file ("
								+ date_from_file
								+ ") does not meet the required format ("
								+ internalDateFormat
								+ ")", e);
					}

					LOG.info("Last value loaded from file: " + last_date);
				}else{
					LOG.info("File for loading last value is empty");
				}
			}else{
				committing_file.createNewFile();

				LOG.info("File for storing last commited value has been created: " +
						committing_file.getAbsolutePath());
			}
		} catch (IOException e) {
			throw new FlumeException(e);
		}
	}

	public List<Event> readEvents(int numberOfEventToRead) throws IOException {
		if(state != State.CONFIGURED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not configured");

		LinkedList<Event> events = new LinkedList<Event>();

		for (int i = 0; i < numberOfEventToRead; i++){
			Event event = readEvent();

			if(event != null){
				events.add(event);
			}else{
				LOG.debug("Number of events returned: " + events.size());
				return events;
			}
		}

		LOG.debug("Number of events returned: " + events.size());
		return events;
	}

	public Event readEvent() {
		if(state != State.CONFIGURED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not configured");
		
		int skippedEventsBecauseNewerTimestamps = 0;
		
		LogEvent event = null;
		Event flumeEvent = null;
		while((event = logFile.getNextEvent()) != null){
			if(last_date == null || event.getTimestamp().getTime() >= last_date.getTime()){
				LOG.trace("New event: " + event);
		    	
				flumeEvent = parser.parse(event);
				
				last_date = event.getTimestamp();

				break;
			}else{
				skippedEventsBecauseNewerTimestamps++;
				
				LOG.trace("Event skipped because timestamp was older than the timestamp of previos events ("
						+ internalDateFormat.format(last_date)
						+ "): " + event);
			}
		}
		
		if(skippedEventsBecauseNewerTimestamps > 0){
			LOG.warn("Several events ("
					+ skippedEventsBecauseNewerTimestamps
					+ ") has been skipped because timestamp was older than the timestamp of previous events ("
					+ internalDateFormat.format(last_date)
					+ ")");
		}
		
		return flumeEvent;
	}
	
	public void commit() throws IOException {
		if(state != State.CONFIGURED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not configured");

		if(last_date == null)
			return;

		FileWriter out = new FileWriter(committing_file, false);
		out.write(internalDateFormat.format(last_date));
		out.close();
		
		last_committed_date = last_date;
	}

	public void rollback() {
		LOG.warn("Rolling back...");
		
		last_date = last_committed_date;
	
		logFile.close();
	}
	
	public void close() {
		logFile.close();
	}
	
}
