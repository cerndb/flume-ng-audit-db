package ch.cern.db.flume.source.deserializer;

import java.nio.charset.Charset;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import ch.cern.db.flume.AuditEvent;
import ch.cern.db.flume.AuditEvent.Field;

import com.google.gson.JsonObject;

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
			
			if(field.value instanceof Number)
				json.addProperty(field.name, (Number) field.value);
			else if(field.value instanceof Boolean){
				json.addProperty(field.name, (Boolean) field.value);
			}else{
				json.addProperty(field.name, (String) field.value);
			}
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
