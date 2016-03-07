package ch.cern.db.flume.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
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
	
	public static final String PATH_PARAM = PARAM + ".path";
	private File committing_file = null;
	
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
		
		String path = context.getString(PATH_PARAM);
		if(path != null){
			File file = new File(path);
			
			if(this.committing_file == null || !this.committing_file.equals(path)){
				this.committing_file = file;
				loadLastHashesFromFile();
			}
		}else{
			this.committing_file = null;
		}

		LOG.info("Configured with size="+size+", headers="+checkHeaders+", body="+checkBody+", path="+path);
	}
	
	private void loadLastHashesFromFile() {
		if(committing_file == null)
			return;
		
		try {
			if(committing_file.exists()){
				previous_hashes.clear();
				
				BufferedReader br = new BufferedReader(new FileReader(committing_file));
				String line = null;
				while ((line = br.readLine()) != null) {
					int hash = Integer.parseInt(line);
					
					previous_hashes.add(hash);
				}
				br.close();
				
				LOG.info("Last hashes loaded from file: " + committing_file);
			}else{
				LOG.info("File for storing last hashes does not exist (" +
						committing_file.getAbsolutePath() + "). Therefore hashes list is not modified");
			}
		} catch (IOException e) {
			throw new FlumeException(e);
		}
	}
	
	public void commit() {
		if(committing_file != null){
			try {
				FileWriter fw = new FileWriter(committing_file);
				
				for(Integer hash:previous_hashes.getInmutableList())
					fw.write(hash.toString() + System.lineSeparator());
				for(Integer hash:hashes_current_batch.getInmutableList())
					fw.write(hash.toString() + System.lineSeparator());
				
				fw.close();
			} catch (IOException e) {
				throw new FlumeException(e);
			}
		}
		
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
