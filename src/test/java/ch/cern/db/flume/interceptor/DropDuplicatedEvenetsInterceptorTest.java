/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.interceptor;

import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.flume.interceptor.DropDuplicatedEventsInterceptor.Builder;

@SuppressWarnings("deprecation")
public class DropDuplicatedEvenetsInterceptorTest {

	@Test
	public void differentBody(){
		Builder interceptor_builder = new DropDuplicatedEventsInterceptor.Builder();
		Context context = new Context();
		context.put("size", "2");
		interceptor_builder.configure(context);
		
		Interceptor interceptor = interceptor_builder.build();
		interceptor.initialize();
		
		//Batch 1
		LinkedList<Event> b1_events = new LinkedList<Event>();
		Event e1 = EventBuilder.withBody("1111".getBytes());
		b1_events.add(e1);
		Event e2 = EventBuilder.withBody("2222".getBytes());
		b1_events.add(e2);
		List<Event> b1_events_intercepted = interceptor.intercept(b1_events);
		Assert.assertEquals(2, b1_events_intercepted.size());
		Assert.assertSame(e1, b1_events_intercepted.get(0));
		Assert.assertSame(e2, b1_events_intercepted.get(1));
		
		//Batch 2
		LinkedList<Event> b2_events = new LinkedList<Event>();
		Event e3 = EventBuilder.withBody("3333".getBytes());
		b2_events.add(e3);
		Event e4 = EventBuilder.withBody("2222".getBytes());
		b2_events.add(e4);
		List<Event> b2_events_intercepted = interceptor.intercept(b2_events);
		Assert.assertEquals(1, b2_events_intercepted.size());
		Assert.assertSame(e3, b2_events_intercepted.get(0));
		
		//Batch 3
		LinkedList<Event> b3_events = new LinkedList<Event>();
		Event e5 = EventBuilder.withBody("1111".getBytes());
		b3_events.add(e5);
		Event e6 = EventBuilder.withBody("2222".getBytes());
		b3_events.add(e6);
		List<Event> b3_events_intercepted = interceptor.intercept(b3_events);
		Assert.assertEquals(2, b3_events_intercepted.size());
		Assert.assertSame(e5, b3_events_intercepted.get(0));
		Assert.assertSame(e6, b3_events_intercepted.get(1));
	}
	
}