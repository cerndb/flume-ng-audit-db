package ch.cern.db.audit.flume.source.deserilizer;

import org.apache.flume.Event;

import ch.cern.db.audit.flume.AuditEvent;

public interface AuditEventDeserialezer {

	public Event process(AuditEvent event);
	
}
