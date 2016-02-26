package ch.cern.db.flume.interceptor;

import org.apache.flume.Event;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.flume.JSONEvent;

public class JSONEventToCSVInterceptorTest {

	@Test
	public void parse(){
		JSONEventToCSVInterceptor interceptor = 
				(JSONEventToCSVInterceptor) new JSONEventToCSVInterceptor.Builder().build();
		
		JSONEvent jsonEvent = new JSONEvent();
		jsonEvent.addProperty("name", "Daniel");
		jsonEvent.addProperty("age", 26);
		jsonEvent.addProperty("single", true);
		
		Event csvEvent = interceptor.intercept(jsonEvent);
		
		Assert.assertEquals("\"Daniel\",26,true", new String(csvEvent.getBody()));
	}
	
}
