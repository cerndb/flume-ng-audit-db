package ch.cern.db.audit.flume.source.deserilizer;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

import ch.cern.db.audit.flume.AuditEvent;

public interface AuditEventDeserializer {

	public Event process(AuditEvent event);

	/** Builder implementations MUST have a no-arg constructor */
	public interface Builder extends Configurable {
		public AuditEventDeserializer build();
	}
}
