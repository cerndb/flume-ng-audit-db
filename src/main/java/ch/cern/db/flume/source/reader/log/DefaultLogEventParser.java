package ch.cern.db.flume.source.reader.log;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import ch.cern.db.log.LogEvent;

public class DefaultLogEventParser implements LogEventParser {
	
	public static final String TIMESTAMP_HEADER_NAME = "log_event_timestamp";
	
	private SimpleDateFormat internalDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

	public DefaultLogEventParser(Context context) {
	}

	@Override
	public Event parse(LogEvent logEvent) {
		
		//Body with event text
		Event flumeEvent = EventBuilder.withBody(logEvent.getText().getBytes());
		
		//Add event timestamp to headers
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(TIMESTAMP_HEADER_NAME, internalDateFormat.format(logEvent.getTimestamp()));
		flumeEvent.setHeaders(headers);
		
		return flumeEvent;
	}

	/**
	 * Builder which builds new instance of this class
	 */
	public static class Builder implements LogEventParser.Builder {

		@Override
		public LogEventParser build(Context context) {
			return new DefaultLogEventParser(context);
		}

	}
	
}
