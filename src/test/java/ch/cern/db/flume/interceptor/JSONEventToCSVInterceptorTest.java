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

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
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
	
	@Test
	public void mixedEvents(){
		JSONEventToCSVInterceptor interceptor = 
				(JSONEventToCSVInterceptor) new JSONEventToCSVInterceptor.Builder().build();
		
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
		
		Assert.assertSame(e1, intercepted_events.get(0));
		Assert.assertEquals("\"Daniel\",26,true", new String(intercepted_events.get(1).getBody()));
		Assert.assertSame(e3, intercepted_events.get(2));
		Assert.assertEquals("\"Paco\",16", new String(intercepted_events.get(3).getBody()));
	}
	
}
