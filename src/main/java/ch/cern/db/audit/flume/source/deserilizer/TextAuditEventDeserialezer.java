package ch.cern.db.audit.flume.source.deserilizer;

import java.nio.charset.Charset;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import ch.cern.db.audit.flume.AuditEvent;

public class TextAuditEventDeserialezer implements AuditEventDeserialezer {

	@Override
	public Event process(AuditEvent event) {
		return EventBuilder.withBody(event.toString(), Charset.defaultCharset());
	}

}
