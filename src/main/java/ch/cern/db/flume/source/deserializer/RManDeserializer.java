/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.source.deserializer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import ch.cern.db.flume.JSONEvent;
import ch.cern.db.utils.SUtils;

/**
 * A deserializer that parses text lines from a file.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RManDeserializer implements EventDeserializer {

	private static final Logger logger = LoggerFactory.getLogger(RManDeserializer.class);

	private final ResettableInputStream in;
	private final int maxLineLength;
	private volatile boolean isOpen;

	public static final String OUT_CHARSET_KEY = "outputCharset";
	public static final String CHARSET_DFLT = "UTF-8";

	public static final String MAXLINE_KEY = "maxLineLength";
	public static final int MAXLINE_DFLT = 2048;

	private static final DateFormat dateFormatter = new SimpleDateFormat("'['EEE MMM dd HH:mm:ss z yyyy']'");
	
	private static final Pattern propertyPattern = Pattern.compile("^([A-z,0-9]+)[ ]+=[ ]+(.+)");
	private static final Pattern ORAPattern = Pattern.compile("^[ ]?ORA-\\d{5}[:][ ].*");
	private static final Pattern RMANPattern = Pattern.compile("^[ ]?RMAN-\\d{5}[:][ ].*");
	
	RManDeserializer(Context context, ResettableInputStream in) {
		this.in = in;
		this.maxLineLength = context.getInteger(MAXLINE_KEY, MAXLINE_DFLT);
		this.isOpen = true;
	}

	/**
	 * Reads a line from a file and returns an event
	 * 
	 * @return Event containing parsed line
	 * @throws IOException
	 */
	@Override
	public Event readEvent() throws IOException {
		ensureOpen();
		
		JSONEvent event = new JSONEvent();
		
		List<String> lines = readAllLines();
		
		String[] fieldsFirstLine = SUtils.grep(lines, "^\\[.*").get(0).split("\\s+(?![^\\[]*\\])");
		
		Date startTimestamp = null;
		try {
			startTimestamp = (Date) dateFormatter.parse(fieldsFirstLine[0]);
		} catch (ParseException e) {
			logger.error("When parsing timestamp: " + fieldsFirstLine[0], e);
		}
		event.addProperty("startTimestamp", startTimestamp);
		
		String backupType = fieldsFirstLine[1];
		event.addProperty("backupType", backupType);
		
		String entityNmae = fieldsFirstLine[2];
		event.addProperty("entityNmae", entityNmae);
		
		//Process properties like (name = value)
		for (String line : lines) {
			Matcher m = propertyPattern.matcher(line);
			
			if(m.find())
				event.addProperty(m.group(1), m.group(2));
		}
		
		//Process RMAN-XXXXX
		JsonArray rmanErrors = new JsonArray();
		for (String line : SUtils.grep(lines, RMANPattern)) {
			JsonObject element = new JsonObject();
			String[] splitted = line.trim().split(":", 2);
			
			element.addProperty("id", Integer.valueOf(splitted[0].split("-")[1]));
			element.addProperty("message", splitted[1]);
			
			rmanErrors.add(element);
		}
		event.addProperty("RMAN-", rmanErrors);
		
		//Process ORA-XXXXX
		JsonArray oraErrors = new JsonArray();
		for (String line : SUtils.grep(lines, ORAPattern)) {
			JsonObject element = new JsonObject();
			String[] splitted = line.trim().split(":", 2);
				
			element.addProperty("id", Integer.valueOf(splitted[0].split("-")[1]));
			element.addProperty("message", splitted[1]);
				
			oraErrors.add(element);
		}
		event.addProperty("ORA-", oraErrors);
		
		return event;
	}

	/**
	 * Batch line read
	 * 
	 * @param numEvents Ignored 
	 * @return List of one event
	 * @throws IOException
	 */
	@Override
	public List<Event> readEvents(int numEvents) throws IOException {
		ensureOpen();
		
		List<Event> events = Lists.newLinkedList();
		
		Event event = readEvent();
		if (event != null)
			events.add(event);

		return events;
	}

	@Override
	public void mark() throws IOException {
		ensureOpen();
		in.mark();
	}

	@Override
	public void reset() throws IOException {
		ensureOpen();
		in.reset();
	}

	@Override
	public void close() throws IOException {
		if (isOpen) {
			reset();
			in.close();
			isOpen = false;
		}
	}

	private void ensureOpen() {
		if (!isOpen) {
			throw new IllegalStateException("Serializer has been closed");
		}
	}
	
	private List<String> readAllLines() throws IOException {
		List<String> lines = new LinkedList<>();
		
		String line = readLine();
		while(line != null){
			lines.add(line);
			line = readLine();
		}
		
		return lines;
	}

	// TODO: consider not returning a final character that is a high surrogate
	// when truncating
	private String readLine() throws IOException {
		StringBuilder sb = new StringBuilder();
		int c;
		int readChars = 0;
		while ((c = in.readChar()) != -1) {
			readChars++;

			// FIXME: support \r\n
			if (c == '\n') {
				break;
			}

			sb.append((char) c);

			if (readChars >= maxLineLength) {
				logger.warn("Line length exceeds max ({}), truncating line!", maxLineLength);
				break;
			}
		}

		if (readChars > 0) {
			return sb.toString();
		} else {
			return null;
		}
	}

	public static class Builder implements EventDeserializer.Builder {

		@Override
		public EventDeserializer build(Context context, ResettableInputStream in) {
			return new RManDeserializer(context, in);
		}

	}

}
