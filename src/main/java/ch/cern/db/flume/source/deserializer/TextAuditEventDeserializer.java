package ch.cern.db.flume.source.deserializer;

import java.nio.charset.Charset;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import ch.cern.db.flume.AuditEvent;

public class TextAuditEventDeserializer implements AuditEventDeserializer {

	/**
	 * Only builder can instance me
	 */
	private TextAuditEventDeserializer() {
	}
	
	@Override
	public Event process(AuditEvent event) {
		return EventBuilder.withBody(event.toString(), Charset.defaultCharset());
	}

	public static class Builder implements AuditEventDeserializer.Builder {

		@Override
		public void configure(Context context) {
		}

		@Override
		public AuditEventDeserializer build() {
			return new TextAuditEventDeserializer();
		}
		
	}
}
