package ch.cern.db.flume.source;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.db.utils.SizeLimitedHashSet;

/**
 * Compare current event with last events to check it was already processed
 */
public class DropDuplicatedEventsProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(DropDuplicatedEventsProcessor.class);
	
	private boolean checkHeaders;
	private boolean checkBody;
	
	private SizeLimitedHashSet<Integer> previous_hashes;
	
	private SizeLimitedHashSet<Integer> hashes_current_batch;
	
	public DropDuplicatedEventsProcessor(Integer size, Boolean checkHeaders, Boolean checkBody){
		previous_hashes = new SizeLimitedHashSet<Integer>(size);
		hashes_current_batch = new SizeLimitedHashSet<Integer>(size);
		this.checkHeaders = checkHeaders;
		this.checkBody = checkBody;
		
		LOG.info("Configured with size="+size+", headers="+checkHeaders+", body="+checkBody);
	}
	

	public void commit() {
		previous_hashes.addAll(hashes_current_batch.getInmutableList());
		
		hashes_current_batch.clear();
	}
	
	public void rollback() {
		hashes_current_batch.clear();
	}

	public Event process(Event event) {
		int event_hash = hashCode(event);
		
		if(previous_hashes.contains(event_hash)
				|| hashes_current_batch.contains(event_hash))
			return null;
		
		hashes_current_batch.add(event_hash);
		return event;
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

	public List<Event> process(List<Event> events) {
		LinkedList<Event> intercepted_events = new LinkedList<Event>();
		
		for (Event event : events) {
			Event intercepted_event = process(event);
			if(intercepted_event != null)
				intercepted_events.add(event);
		}
		
		return intercepted_events;
	}

	public void close() {
		previous_hashes.clear();
		hashes_current_batch.clear();
	}

}
