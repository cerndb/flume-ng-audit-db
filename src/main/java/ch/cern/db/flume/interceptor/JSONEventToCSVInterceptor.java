package ch.cern.db.flume.interceptor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;

import ch.cern.db.flume.JSONEvent;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * 
 * 
 * @author daniellanzagarcia
 *
 */
public class JSONEventToCSVInterceptor implements Interceptor {

	private JSONEventToCSVInterceptor(){
	}
	
	@Override
	public void initialize() {
	}

	@Override
	public Event intercept(Event event) {
		if(!(event instanceof JSONEvent))
			return event;
		
		StringBuilder csv = new StringBuilder();
		
		JsonObject json = ((JSONEvent) event).getJsonObject();
		boolean first = true;
		for (Entry<String, JsonElement> property : json.entrySet()) {
			if(first)
				first = false;
			else
				csv.append(",");
			
			csv.append(property.getValue());
		}
		
		return EventBuilder.withBody(csv.toString().getBytes(), event.getHeaders());
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		LinkedList<Event> intercepted = new LinkedList<Event>();
		
		for (Event event : events) {
			intercepted.add(intercept(event));
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
			return new JSONEventToCSVInterceptor();
		}

	}
	
}
