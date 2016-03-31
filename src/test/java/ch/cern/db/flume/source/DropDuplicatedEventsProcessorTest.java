/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.source;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class DropDuplicatedEventsProcessorTest {
	
	@Test
	public void reconfigure(){
		
		Context context = new Context();
		context.put(DropDuplicatedEventsProcessor.SIZE_PARAM, "1");
		DropDuplicatedEventsProcessor interceptor = new DropDuplicatedEventsProcessor();
		interceptor.configure(context);
		
		//Batch 1
		LinkedList<Event> b1_events = new LinkedList<Event>();
		Event e1 = EventBuilder.withBody("1111".getBytes());
		b1_events.add(e1);
		Event e2 = EventBuilder.withBody("2222".getBytes());
		b1_events.add(e2);
		Event e3 = EventBuilder.withBody("1111".getBytes());
		b1_events.add(e3);
		List<Event> b1_events_intercepted = interceptor.process(b1_events);
		Assert.assertEquals(3, b1_events_intercepted.size());
		Assert.assertSame(e1, b1_events_intercepted.get(0));
		Assert.assertSame(e2, b1_events_intercepted.get(1));
		Assert.assertSame(e3, b1_events_intercepted.get(2));
		
		context.put(DropDuplicatedEventsProcessor.SIZE_PARAM, "2");
		interceptor.configure(context);
		
		LinkedList<Event> b2_events = new LinkedList<Event>();
		Event e4 = EventBuilder.withBody("1111".getBytes());
		b2_events.add(e4);
		Event e5 = EventBuilder.withBody("2222".getBytes());
		b2_events.add(e5);
		Event e6 = EventBuilder.withBody("1111".getBytes());
		b2_events.add(e6);
		Event e7 = EventBuilder.withBody("2222".getBytes());
		b2_events.add(e7);
		List<Event> b2_events_intercepted = interceptor.process(b2_events);
		Assert.assertEquals(1, b2_events_intercepted.size());
		Assert.assertSame(e5, b2_events_intercepted.get(0));
	}

	@Test
	public void sameBatchEvents(){
		
		Context context = new Context();
		context.put(DropDuplicatedEventsProcessor.SIZE_PARAM, "2");
		DropDuplicatedEventsProcessor interceptor = new DropDuplicatedEventsProcessor();
		interceptor.configure(context);
		
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
		Event e5 = EventBuilder.withBody("4444".getBytes());
		b3_events.add(e5);
		Event e6 = EventBuilder.withBody("4444".getBytes());
		b3_events.add(e6);
		List<Event> b3_events_intercepted = interceptor.process(b3_events);
		Assert.assertEquals(1, b3_events_intercepted.size());
		Assert.assertSame(e5, b3_events_intercepted.get(0));
	}
	
	@Test
	public void differentBatchEvents(){
		
		Context context = new Context();
		context.put(DropDuplicatedEventsProcessor.SIZE_PARAM, "2");
		DropDuplicatedEventsProcessor interceptor = new DropDuplicatedEventsProcessor();
		interceptor.configure(context);
		
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
		
		interceptor.commit();
		// 2222 1111
		
		//Batch 2
		LinkedList<Event> b2_events = new LinkedList<Event>();
		Event e3 = EventBuilder.withBody("3333".getBytes());
		b2_events.add(e3);
		Event e4 = EventBuilder.withBody("2222".getBytes());
		b2_events.add(e4);
		List<Event> b2_events_intercepted = interceptor.process(b2_events);
		Assert.assertEquals(1, b2_events_intercepted.size());
		Assert.assertSame(e3, b2_events_intercepted.get(0));
		
		interceptor.commit();
		// 3333 2222
		
		//Batch 3
		LinkedList<Event> b3_events = new LinkedList<Event>();
		Event e5 = EventBuilder.withBody("1111".getBytes());
		b3_events.add(e5);
		Event e6 = EventBuilder.withBody("2222".getBytes());
		b3_events.add(e6);
		Event e7 = EventBuilder.withBody("3333".getBytes());
		b3_events.add(e7);
		List<Event> b3_events_intercepted = interceptor.process(b3_events);
		Assert.assertEquals(1, b3_events_intercepted.size());
		Assert.assertSame(e5, b3_events_intercepted.get(0));
	}
	
	@Test
	public void rollback(){
		
		Context context = new Context();
		context.put(DropDuplicatedEventsProcessor.SIZE_PARAM, "2");
		DropDuplicatedEventsProcessor interceptor = new DropDuplicatedEventsProcessor();
		interceptor.configure(context);
		
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
		
		interceptor.commit();
		// 2222 1111
		
		//Batch 2
		LinkedList<Event> b2_events = new LinkedList<Event>();
		Event e3 = EventBuilder.withBody("3333".getBytes());
		b2_events.add(e3);
		Event e4 = EventBuilder.withBody("4444".getBytes());
		b2_events.add(e4);
		List<Event> b2_events_intercepted = interceptor.process(b2_events);
		Assert.assertEquals(2, b2_events_intercepted.size());
		Assert.assertSame(e3, b2_events_intercepted.get(0));
		Assert.assertSame(e4, b2_events_intercepted.get(1));
		
		interceptor.rollback();
		// 2222 1111
		
		//Batch 3
		LinkedList<Event> b3_events = new LinkedList<Event>();
		Event e5 = EventBuilder.withBody("3333".getBytes());
		b3_events.add(e5);
		Event e6 = EventBuilder.withBody("2222".getBytes());
		b3_events.add(e6);
		Event e7 = EventBuilder.withBody("1111".getBytes());
		b3_events.add(e7);
		List<Event> b3_events_intercepted = interceptor.process(b3_events);
		Assert.assertEquals(1, b3_events_intercepted.size());
		Assert.assertSame(e5, b3_events_intercepted.get(0));
	}
	
	@Test
	public void persistingHash(){
		new File("src/test/resources/last.hashes").delete();
		
		Context context = new Context();
		context.put(DropDuplicatedEventsProcessor.SIZE_PARAM, "2");
		context.put(DropDuplicatedEventsProcessor.PATH_PARAM, "src/test/resources/last.hashes");
		DropDuplicatedEventsProcessor interceptor = new DropDuplicatedEventsProcessor();
		interceptor.configure(context);
		
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
		
		interceptor.commit();
		interceptor = new DropDuplicatedEventsProcessor();
		interceptor.configure(context);
		//2222 		1111
		//[2462721, 2431937]
		
		//Batch 2
		LinkedList<Event> b2_events = new LinkedList<Event>();
		Event e3 = EventBuilder.withBody("3333".getBytes());
		b2_events.add(e3);
		Event e4 = EventBuilder.withBody("2222".getBytes());
		b2_events.add(e4);
		List<Event> b2_events_intercepted = interceptor.process(b2_events);
		Assert.assertEquals(1, b2_events_intercepted.size());
		Assert.assertSame(e3, b2_events_intercepted.get(0));
		
		interceptor.commit();
		interceptor = new DropDuplicatedEventsProcessor();
		interceptor.configure(context);
		//3333 2222
		//[2493505, 2462721] 
		
		//Batch 3
		LinkedList<Event> b3_events = new LinkedList<Event>();
		Event e5 = EventBuilder.withBody("1111".getBytes());
		b3_events.add(e5);
		Event e6 = EventBuilder.withBody("2222".getBytes());
		b3_events.add(e6);
		Event e7 = EventBuilder.withBody("3333".getBytes());
		b3_events.add(e7);
		List<Event> b3_events_intercepted = interceptor.process(b3_events);
		Assert.assertEquals(1, b3_events_intercepted.size());
		Assert.assertSame(e5, b3_events_intercepted.get(0));
	}
	
	@After
	public void cleanUp(){
		new File(DropDuplicatedEventsProcessor.PATH_DEFAULT).delete();
	}
	
}