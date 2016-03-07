package ch.cern.db.flume.source;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.db.utils.SizeLimitedHashSet;

/**
 * Compare current event with last events to check it was already processed
 */
public class DropDuplicatedEventsProcessor implements Configurable{

	private static final Logger LOG = LoggerFactory.getLogger(DropDuplicatedEventsProcessor.class);
	
	public static final String PARAM = "duplicatedEventsProcessor";
	
	public static final String SIZE_PARAM = PARAM + ".size";
	public static final Integer SIZE_DEFAULT = 1000;
	
	public static final String CHECK_HEADER_PARAM = PARAM + ".header";
	public static final Boolean CHECK_HEADER_DEFAULT = true;
	private boolean checkHeaders;
	
	public static final String CHECK_BODY_PARAM = PARAM + ".body";
	public static final Boolean CHECK_BODY_DEFAULT = true;
	private boolean checkBody;
	
	private SizeLimitedHashSet<Integer> previous_hashes;
	
	private SizeLimitedHashSet<Integer> hashes_current_batch;
	
	public DropDuplicatedEventsProcessor(){
		Integer size = SIZE_DEFAULT;
		previous_hashes = new SizeLimitedHashSet<Integer>(SIZE_DEFAULT);
		hashes_current_batch = new SizeLimitedHashSet<Integer>(SIZE_DEFAULT);
		
		this.checkHeaders = CHECK_HEADER_DEFAULT;
		this.checkBody = CHECK_BODY_DEFAULT;
		
		LOG.info("Initializated with defaults size="+size+", headers="+checkHeaders+", body="+checkBody);
	}
	
	@Override
	public void configure(Context context){
		Integer size = context.getInteger(SIZE_PARAM, SIZE_DEFAULT);
		if(size != previous_hashes.size()){
			SizeLimitedHashSet<Integer> tmp = previous_hashes;
			previous_hashes = new SizeLimitedHashSet<Integer>(size);
			previous_hashes.addAll(tmp.getInmutableList());
			tmp.clear();
			
			tmp = hashes_current_batch;
			hashes_current_batch = new SizeLimitedHashSet<Integer>(size);
			hashes_current_batch.addAll(tmp.getInmutableList());
			tmp.clear();
		}
		
		this.checkHeaders = context.getBoolean(CHECK_HEADER_PARAM, CHECK_HEADER_DEFAULT);
		this.checkBody = context.getBoolean(CHECK_BODY_PARAM, CHECK_BODY_DEFAULT);
		
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
