package ch.cern.db.flume.interceptor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.interceptor.Interceptor;

/**
 * This intercepted works only if source puts
 * events into batches (getChannelProcessor().processEventBatch(events))
 * not single events.
 */
public class DropDuplicatedEventsInterceptor implements Interceptor {

	private boolean checkHeaders = true;
	private boolean checkBody = true;
	
	private HashSet<Integer> hashes_previous_batch;
	private HashSet<Integer> hashes_current_batch;
	
	private DropDuplicatedEventsInterceptor(Context context){
		checkHeaders = context.getBoolean("headers", true);
		checkBody = context.getBoolean("body", true);
	}
	
	@Override
	public void initialize() {
		hashes_previous_batch = new HashSet<Integer>();
		hashes_current_batch = new HashSet<Integer>();
	}

	@Override
	public Event intercept(Event event) {
		throw new FlumeException(this.getClass().getName()+" is only compatible with sources that process events in batches");
	}

	private int hashCode(Event event) {
		int headers_hash = 0;
		if(checkHeaders)
			headers_hash = event.getHeaders().hashCode();
		
		int body_hash = 0;
		if(checkBody)
			body_hash = Arrays.hashCode(event.getBody());
		
		if(checkHeaders && checkBody)
			return headers_hash ^ body_hash;
		if (checkHeaders && !checkBody)
			return headers_hash;
		if (!checkHeaders && checkBody)
			return body_hash;
		//else all events will be removed due to hash will be always 0
			return 0;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		hashes_previous_batch = hashes_current_batch;
		hashes_current_batch = new HashSet<Integer>();
		
		LinkedList<Event> intercepted_events = new LinkedList<Event>();
		
		for (Event event : events) {
			int event_hash = hashCode(event);
			
			hashes_current_batch.add(event_hash);
			
			Event intercepted_event = hashes_previous_batch.contains(event_hash) ? null : event;;
			if(intercepted_event != null)
				intercepted_events.add(event);
		}
		
		return intercepted_events;
	}

	@Override
	public void close() {
	}

	/**
	 * Builder which builds new instance of this class
	 */
	public static class Builder implements Interceptor.Builder {

		private Context context;
		
		@Override
		public void configure(Context context) {
			this.context = context;
		}

		@Override
		public Interceptor build() {
			return new DropDuplicatedEventsInterceptor(context);
		}

	}
	
}
