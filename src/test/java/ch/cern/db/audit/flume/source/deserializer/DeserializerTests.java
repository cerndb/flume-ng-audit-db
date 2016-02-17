package ch.cern.db.audit.flume.source.deserializer;

import org.apache.flume.Event;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.audit.flume.AuditEvent;


public class DeserializerTests {

	@Test
	public void toJSON(){
		
		AuditEvent event = new AuditEvent();
		event.addField("boolean", (boolean) true);
		event.addField("Boolean", (Boolean) true);
		event.addField("int", (int) 1);
		event.addField("Integer", (Integer) 1);
		event.addField("float", (float) 1);
		event.addField("Float", (Float) 1.0F);
		event.addField("double", (double) 1);
		event.addField("Double", (Double) 1.0D);
		event.addField("String", "example");
		
		JSONAuditEventDeserializer deserializer = (JSONAuditEventDeserializer) new JSONAuditEventDeserializer.Builder().build();
		
		Event deserialized = deserializer.process(event);
		
		Assert.assertEquals(new String(deserialized.getBody()), 
				"{\"boolean\":true,\"Boolean\":true,"
				+ "\"int\":1,\"Integer\":1,"
				+ "\"float\":1.0,\"Float\":1.0,"
				+ "\"double\":1.0,\"Double\":1.0,"
				+ "\"String\":\"example\"}");
	}
	
	@Test
	public void toText(){
		
		AuditEvent event = new AuditEvent();
		event.addField("boolean", (boolean) true);
		event.addField("Boolean", (Boolean) true);
		event.addField("int", (int) 1);
		event.addField("Integer", (Integer) 1);
		event.addField("float", (float) 1);
		event.addField("Float", (Float) 1.0F);
		event.addField("double", (double) 1);
		event.addField("Double", (Double) 1.0D);
		event.addField("String", "example");
		
		TextAuditEventDeserializer deserializer = (TextAuditEventDeserializer) new TextAuditEventDeserializer.Builder().build();
		
		Event deserialized = deserializer.process(event);
		
		Assert.assertEquals(new String(deserialized.getBody()), 
				"AuditEvent [fields=["
				+ "Field [name=boolean, value=true], Field [name=Boolean, value=true], "
				+ "Field [name=int, value=1], Field [name=Integer, value=1], "
				+ "Field [name=float, value=1.0], Field [name=Float, value=1.0], "
				+ "Field [name=double, value=1.0], Field [name=Double, value=1.0], "
				+ "Field [name=String, value=example]]]");
	}
	
}
