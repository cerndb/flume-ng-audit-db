package ch.cern.flume.source.oracle;

import java.nio.charset.Charset;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

public class TextAuditEventDeserialezer implements AuditEventDeserialezer {

	@Override
	public Event process(AuditEvent event) {
		return EventBuilder.withBody(event.toString(), Charset.defaultCharset());
	}

}
