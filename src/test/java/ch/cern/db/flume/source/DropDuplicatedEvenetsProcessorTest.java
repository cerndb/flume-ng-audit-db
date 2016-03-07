package ch.cern.db.flume.source;

import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;

public class DropDuplicatedEvenetsProcessorTest {

	@Test
	public void differentBody(){
		DropDuplicatedEventsProcessor interceptor = new DropDuplicatedEventsProcessor(2, true, true);
		
		//Batch 1
		LinkedList<Event> b1_events = new LinkedList<Event>();
		Event e1 = EventBuilder.withBody("1111".getBytes());
		b1_events.add(e1);
		Event e2 = EventBuilder.withBody("2222".getBytes());
		b1_events.add(e2);
		List<Event> b1_events_intercepted = interceptor.process(b1_events);
		Assert.assertEquals(2, b1_events_intercepted.size());
		Assert.assertSame(e1, b1_events_intercepted.get(0));
		Assert.assertSame(e2, b1_events_intercepted.get(1));
		
		//Batch 2
		LinkedList<Event> b2_events = new LinkedList<Event>();
		Event e3 = EventBuilder.withBody("3333".getBytes());
		b2_events.add(e3);
		Event e4 = EventBuilder.withBody("2222".getBytes());
		b2_events.add(e4);
		List<Event> b2_events_intercepted = interceptor.process(b2_events);
		Assert.assertEquals(1, b2_events_intercepted.size());
		Assert.assertSame(e3, b2_events_intercepted.get(0));
		
		//Batch 3
		LinkedList<Event> b3_events = new LinkedList<Event>();
		Event e5 = EventBuilder.withBody("1111".getBytes());
		b3_events.add(e5);
		Event e6 = EventBuilder.withBody("2222".getBytes());
		b3_events.add(e6);
		List<Event> b3_events_intercepted = interceptor.process(b3_events);
		Assert.assertEquals(2, b3_events_intercepted.size());
		Assert.assertSame(e5, b3_events_intercepted.get(0));
		Assert.assertSame(e6, b3_events_intercepted.get(1));
	}
	
}
