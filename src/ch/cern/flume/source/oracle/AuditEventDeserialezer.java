package ch.cern.flume.source.oracle;

import org.apache.flume.Event;

public interface AuditEventDeserialezer {

	public Event process(AuditEvent event);
	
}
