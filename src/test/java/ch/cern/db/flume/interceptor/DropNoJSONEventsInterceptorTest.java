package ch.cern.db.flume.interceptor;

import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.flume.JSONEvent;

public class DropNoJSONEventsInterceptorTest {
	
	@Test
	public void filter(){
		DropNoJSONEventsInterceptor interceptor = 
				(DropNoJSONEventsInterceptor) new DropNoJSONEventsInterceptor.Builder().build();
		
		LinkedList<Event> events = new LinkedList<Event>();
		Event e1 = EventBuilder.withBody("aa".getBytes());
		events.add(e1);
		
		JSONEvent e2 = new JSONEvent();
		e2.addProperty("name", "Daniel");
		e2.addProperty("age", 26);
		e2.addProperty("single", true);
		events.add(e2);
		
		Event e3 = EventBuilder.withBody("bb".getBytes());
		events.add(e3);
		
		JSONEvent e4 = new JSONEvent();
		e4.addProperty("name", "Paco");
		e4.addProperty("age", 16);
		events.add(e4);
		
		List<Event> intercepted_events = interceptor.intercept(events);
		
		Assert.assertSame(e2, intercepted_events.get(0));
		Assert.assertSame(e4, intercepted_events.get(1));
		Assert.assertEquals(2, intercepted_events.size());
	}
	
}
