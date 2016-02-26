package ch.cern.db.flume.interceptor;

import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import ch.cern.db.flume.JSONEvent;

/**
 * 
 * 
 * @author daniellanzagarcia
 *
 */
public class DropNoJSONEventsInterceptor implements Interceptor {

	private DropNoJSONEventsInterceptor(){
	}
	
	@Override
	public void initialize() {
	}

	@Override
	public Event intercept(Event event) {
		if(event instanceof JSONEvent)
			return event;
		else
			return null;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		LinkedList<Event> intercepted = new LinkedList<Event>();
		
		for (Event event : events) {
			Event intercepted_event = intercept(event);
			
			if(intercepted_event != null)
				intercepted.add(event);
		}
		
		return intercepted;
	}

	@Override
	public void close() {
	}

	/**
	 * Builder which builds new instance of this class
	 */
	public static class Builder implements Interceptor.Builder {

		@Override
		public void configure(Context context) {
		}

		@Override
		public Interceptor build() {
			return new DropNoJSONEventsInterceptor();
		}

	}
	
}
