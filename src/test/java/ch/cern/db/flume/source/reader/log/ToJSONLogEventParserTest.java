package ch.cern.db.flume.source.reader.log;

import java.text.ParseException;
import java.util.Date;

import org.apache.flume.Event;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.log.LogEvent;

public class ToJSONLogEventParserTest {

	@Test
	public void basic() throws ParseException {
		ToJSONLogEventParser parser = new ToJSONLogEventParser(null);
		
		Date timestamp = ToJSONLogEventParser.internalDateFormat.parse("2016-09-16T15:58:21+0200");
		String text = "Event text";
		LogEvent logEvent = new LogEvent(timestamp, text);
		
		Event event = parser.parse(logEvent);
		
		Assert.assertEquals("{\"event_timestamp\":\"2016-09-16T15:58:21+0200\",\"text\":\"Event text\"}", 
				new String(event.getBody()));
	}
	
	@Test
	public void severalEvents() throws ParseException {
		ToJSONLogEventParser parser = new ToJSONLogEventParser(null);
		
		Date timestamp = ToJSONLogEventParser.internalDateFormat.parse("2016-09-16T15:58:21+0200");
		String text = "Event 1 text";
		LogEvent logEvent = new LogEvent(timestamp, text);
		
		Event event = parser.parse(logEvent);

		Assert.assertEquals("{\"event_timestamp\":\"2016-09-16T15:58:21+0200\",\"text\":\"Event 1 text\"}", 
				new String(event.getBody()));
		
		timestamp = ToJSONLogEventParser.internalDateFormat.parse("2016-09-02T11:08:01+0200");
		text = "Event 2 text";
		logEvent = new LogEvent(timestamp, text);
		
		event = parser.parse(logEvent);

		Assert.assertEquals("{\"event_timestamp\":\"2016-09-02T11:08:01+0200\",\"text\":\"Event 2 text\"}", 
				new String(event.getBody()));
		
		timestamp = ToJSONLogEventParser.internalDateFormat.parse("2016-10-16T15:50:21+0200");
		text = "Event 3 text";
		logEvent = new LogEvent(timestamp, text);
		
		event = parser.parse(logEvent);

		Assert.assertEquals("{\"event_timestamp\":\"2016-10-16T15:50:21+0200\",\"text\":\"Event 3 text\"}", 
				new String(event.getBody()));
	}

}
