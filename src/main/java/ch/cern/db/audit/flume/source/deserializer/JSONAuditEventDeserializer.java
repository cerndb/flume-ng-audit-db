package ch.cern.db.audit.flume.source.deserializer;

import java.nio.charset.Charset;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import com.google.gson.JsonObject;

import ch.cern.db.audit.flume.AuditEvent;
import ch.cern.db.audit.flume.AuditEvent.Field;

public class JSONAuditEventDeserializer implements AuditEventDeserializer {

	/**
	 * Only builder can instance me
	 */
	private JSONAuditEventDeserializer() {
	}
	
	@Override
	public Event process(AuditEvent event) {
		JsonObject json = new JsonObject();
		for(Field field:event.getFields()){
			json.addProperty(field.name, field.value);
		}
		
		return EventBuilder.withBody(json.toString(), Charset.defaultCharset());
	}

	public static class Builder implements AuditEventDeserializer.Builder {

		@Override
		public void configure(Context context) {
		}

		@Override
		public AuditEventDeserializer build() {
			return new JSONAuditEventDeserializer();
		}
		
	}
}
