package ch.cern.db.flume.source.reader.log;

import java.text.SimpleDateFormat;

import org.apache.flume.Context;
import org.apache.flume.Event;

import ch.cern.db.flume.JSONEvent;
import ch.cern.db.log.LogEvent;

public class ToJSONLogEventParser implements LogEventParser{
	
	public static SimpleDateFormat internalDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

	public ToJSONLogEventParser(Context context) {
	}

	@Override
	public Event parse(LogEvent logEvent) {
		JSONEvent event = new JSONEvent();
		
		event.addProperty("event_timestamp", internalDateFormat.format(logEvent.getTimestamp()));
		event.addProperty("text", logEvent.getText());
		
		return event;
	}
	
	/**
	 * Builder which builds new instance of this class
	 */
	public static class Builder implements LogEventParser.Builder {

		@Override
		public LogEventParser build(Context context) {
			return new ToJSONLogEventParser(context);
		}

	}

}
