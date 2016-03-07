package ch.cern.db.flume.source;

import java.io.IOException;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.db.flume.source.reader.ReliableJdbcEventReader;

public class JDBCSource extends AbstractSource implements Configurable, PollableSource {

	private static final Logger LOG = LoggerFactory.getLogger(JDBCSource.class);

	private static final int BATCH_SIZE_DEFAULT = 100;
	private static final String BATCH_SIZE_PARAM = "batch.size";
	private int batch_size = BATCH_SIZE_DEFAULT;

	private static final long MINIMUM_BATCH_TIME_DEFAULT = 10000;
	private static final String MINIMUM_BATCH_TIME_PARAM = "batch.minimumTime";
	private long minimum_batch_time = MINIMUM_BATCH_TIME_DEFAULT;

	private ReliableJdbcEventReader reader;

	private static final String DUPLICATED_EVENTS_PROCESSOR_PARAM = "duplicatedEventsProcessor";
	private static final Boolean DUPLICATED_EVENTS_PROCESSOR_DEFAULT = true;
	private static final String SIZE_DUPLICATED_EVENTS_PROCESSOR_PARAM = 
			DUPLICATED_EVENTS_PROCESSOR_PARAM + ".size";
	private static final Integer SIZE_DUPLICATED_EVENTS_PROCESSOR_DEFAULT = 1000;
	private static final String CHECK_HEADER_DUPLICATED_EVENTS_PROCESSOR_PARAM = 
			DUPLICATED_EVENTS_PROCESSOR_PARAM + ".header";
	private static final Boolean CHECK_HEADER_DUPLICATED_EVENTS_PROCESSOR_DEFAULT = true;
	private static final String CHECK_BODY_DUPLICATED_EVENTS_PROCESSOR_PARAM = 
			DUPLICATED_EVENTS_PROCESSOR_PARAM + ".body";
	private static final Boolean CHECK_BODY_DUPLICATED_EVENTS_PROCESSOR_DEFAULT = true;
	private DropDuplicatedEventsProcessor duplicatedEventsProccesor;
	
	public JDBCSource() {
		super();
		
		reader = new ReliableJdbcEventReader();
	}
	
	@Override
	public void configure(Context context) {
		try{
			String value = context.getString(BATCH_SIZE_PARAM);
			if(value != null)
				batch_size = Integer.parseInt(value);
		}catch(Exception e){
			throw new FlumeException("Configured value for " + BATCH_SIZE_PARAM + " is not a number", e);
		}
		try{
			String value = context.getString(MINIMUM_BATCH_TIME_PARAM);
			if(value != null)
				minimum_batch_time = Integer.parseInt(value);
		}catch(Exception e){
			throw new FlumeException("Configured value for " + MINIMUM_BATCH_TIME_PARAM + " is not a number", e);
		}
		
		reader.configure(context);
		
		if(duplicatedEventsProccesor == null &&
				context.getBoolean(DUPLICATED_EVENTS_PROCESSOR_PARAM, DUPLICATED_EVENTS_PROCESSOR_DEFAULT)){
			duplicatedEventsProccesor = new DropDuplicatedEventsProcessor(
					context.getInteger(SIZE_DUPLICATED_EVENTS_PROCESSOR_PARAM, 
							SIZE_DUPLICATED_EVENTS_PROCESSOR_DEFAULT),
					context.getBoolean(CHECK_HEADER_DUPLICATED_EVENTS_PROCESSOR_PARAM, 
							CHECK_HEADER_DUPLICATED_EVENTS_PROCESSOR_DEFAULT),
					context.getBoolean(CHECK_BODY_DUPLICATED_EVENTS_PROCESSOR_PARAM,
							CHECK_BODY_DUPLICATED_EVENTS_PROCESSOR_DEFAULT));
		}
	}
	
	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		
		long batchStartTime = System.currentTimeMillis();
		
		try{
			List<Event> events = reader.readEvents(batch_size);
			
			events = duplicatedEventsProccesor.process(events);
			
			getChannelProcessor().processEventBatch(events);
			
			reader.commit();
			
			if(duplicatedEventsProccesor != null)
				duplicatedEventsProccesor.commit();
			
			status = Status.READY;
		}catch(Throwable e){
			status = Status.BACKOFF;
			
			if(duplicatedEventsProccesor != null)
				duplicatedEventsProccesor.rollback();
			
			LOG.error(e.getMessage(), e);
			sleep(batchStartTime);
			throw new EventDeliveryException(e);
		}
		
		sleep(batchStartTime);
		
		return status;
	}

	private void sleep(long batchStartTime) {
		long elapsedTime = System.currentTimeMillis() - batchStartTime;
		
		if(elapsedTime <= minimum_batch_time){
			try {
				Thread.sleep(minimum_batch_time - elapsedTime);
			} catch (InterruptedException e) {}
		}
	}

	@Override
	public synchronized void stop() {
		try {
			reader.close();
			
			if(duplicatedEventsProccesor != null)
				duplicatedEventsProccesor.close();
		} catch (IOException e){}
	}

	@Override
	public long getBackOffSleepIncrement() {
		return 0;
	}

	@Override
	public long getMaxBackOffSleepInterval() {
		return 0;
	}

}
