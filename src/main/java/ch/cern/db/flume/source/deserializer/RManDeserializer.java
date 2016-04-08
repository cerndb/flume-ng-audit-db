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
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import ch.cern.db.flume.JSONEvent;
import ch.cern.db.utils.JSONUtils;
import ch.cern.db.utils.Pair;

/**
 * A deserializer that parses text lines from a file.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RManDeserializer implements EventDeserializer {

	private final ResettableInputStream in;
	private final int maxLineLength;
	private volatile boolean isOpen;

	public static final String OUT_CHARSET_KEY = "outputCharset";
	public static final String CHARSET_DFLT = "UTF-8";

	public static final String MAXLINE_KEY = "maxLineLength";
	public static final int MAXLINE_DFLT = 2048;
	
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
		
		RManagerFile rman_log = new RManagerFile(in, maxLineLength);
		
		JSONEvent event = new JSONEvent();

		event.addProperty("startTimestamp", rman_log.getStartTimestamp());
		event.addProperty("backupType", rman_log.getBackupType());
		event.addProperty("entityName", rman_log.getEntityName());
		
		//Process properties like (name = value)
		for (Pair<String, String> property : rman_log.getProperties())
			event.addProperty(property.getFirst(), property.getSecond());
		
		String v_params = rman_log.getVParams();
		event.addProperty("v_params", v_params != null ? new JsonParser().parse(v_params).getAsJsonObject() : null);
		
		String mountPointNASRegexResult = rman_log.getMountPointNASRegexResult();
		event.addProperty("mountPointNASRegex.result", mountPointNASRegexResult != null ?
				new JsonParser().parse(mountPointNASRegexResult).getAsJsonObject() : null);
		
		String volInfoBackuptoDiskFinalResult = rman_log.getVolInfoBackuptoDiskFinalResult();
		event.addProperty("volInfoBackuptoDisk.finalResult", volInfoBackuptoDiskFinalResult != null ?
				new JsonParser().parse(volInfoBackuptoDiskFinalResult).getAsJsonObject() : null);
		
		List<RecoveryManagerReport> recoveryManagerReports = rman_log.getRecoveryManagerReports();
		event.addProperty("recoveryManagerReports", recoveryManagerReportsToJSON(recoveryManagerReports));

		return event;
	}

	private JsonArray recoveryManagerReportsToJSON(List<RecoveryManagerReport> recoveryManagerReports) {
		JsonArray array = new JsonArray();
		
		for (RecoveryManagerReport recoveryManagerReport : recoveryManagerReports) {
			JsonObject element = new JsonObject();
			
			element.addProperty("startingTime", JSONUtils.to(recoveryManagerReport.getStartingTime()));
			
			List<Pair<Integer, String>> rmans = recoveryManagerReport.getRMANs();
			element.add("RMAN-", toJSON(rmans));
			element.add("ORA-", toJSON(recoveryManagerReport.getORAs())); 
			element.addProperty("status", rmans.size() == 0 ? "Successful" : "Failed");
			
			array.add(element);
		}
		
		return array;
	}

	private JsonArray toJSON(List<Pair<Integer, String>> list) {
		JsonArray array = new JsonArray();
		
		for (Pair<Integer, String> rmanError : list) {
			JsonObject element = new JsonObject();
			
			element.addProperty("id", rmanError.getFirst());
			element.addProperty("message", rmanError.getSecond()); 
			
			array.add(element);
		}
		
		return array;
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
	
	public static class Builder implements EventDeserializer.Builder {

		@Override
		public EventDeserializer build(Context context, ResettableInputStream in) {
			return new RManDeserializer(context, in);
		}

	}

}
