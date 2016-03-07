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
		
		if(context.getBoolean(DropDuplicatedEventsProcessor.PARAM, true)){
			if(duplicatedEventsProccesor == null){
				duplicatedEventsProccesor = new DropDuplicatedEventsProcessor();
			}
			duplicatedEventsProccesor.configure(context);
		}else{
			if(duplicatedEventsProccesor != null){
				duplicatedEventsProccesor.close();
				duplicatedEventsProccesor = null;
			}
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
