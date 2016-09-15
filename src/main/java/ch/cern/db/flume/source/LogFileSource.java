/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.source;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import ch.cern.db.flume.source.reader.ReliableLogFileEventReader;

public class LogFileSource extends AbstractSource implements Configurable, PollableSource {

	private static final Logger LOG = LoggerFactory.getLogger(LogFileSource.class);

	public static final int BATCH_SIZE_DEFAULT = 100;
	public static final String BATCH_SIZE_PARAM = "batch.size";
	private int batch_size = BATCH_SIZE_DEFAULT;

	public static final long MINIMUM_BATCH_TIME_DEFAULT = 10000;
	public static final String MINIMUM_BATCH_TIME_PARAM = "batch.minimumTime";
	private long minimum_batch_time = MINIMUM_BATCH_TIME_DEFAULT;

	private ReliableLogFileEventReader reader;

	private DropDuplicatedEventsProcessor duplicatedEventsProccesor;
	
	private SourceCounter sourceCounter;
	
	public LogFileSource() {
		super();
		
		reader = new ReliableLogFileEventReader();
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
		
		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
			sourceCounter.start();
		}
	}
	
	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		
		long batchStartTime = System.currentTimeMillis();
		
		try{
			List<Event> events = reader.readEvents(batch_size);
			
			if(duplicatedEventsProccesor != null)
				events = duplicatedEventsProccesor.process(events);
			
			sourceCounter.addToEventReceivedCount(events.size());
			sourceCounter.incrementAppendBatchReceivedCount();
			
			getChannelProcessor().processEventBatch(events);
			
			reader.commit();
			
			if(duplicatedEventsProccesor != null)
				duplicatedEventsProccesor.commit();
			
			LOG.info("Number of events produced: " + events.size());
			
			sourceCounter.addToEventAcceptedCount(events.size());
			sourceCounter.incrementAppendBatchAcceptedCount();
			
			status = Status.READY;
		}catch(Throwable e){
			LOG.error(e.getMessage(), e);
			
			status = Status.BACKOFF;
			
			reader.rollback();
			
			if(duplicatedEventsProccesor != null)
				duplicatedEventsProccesor.rollback();
			
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
		reader.close();
		
		if(duplicatedEventsProccesor != null)
			duplicatedEventsProccesor.close();
		
		sourceCounter.stop();
		
		LOG.info("JDBCSource {} stopped. Metrics: {}", getName(), sourceCounter);
	}

	@Override
	public long getBackOffSleepIncrement() {
		return 0;
	}

	@Override
	public long getMaxBackOffSleepInterval() {
		return 0;
	}

	@VisibleForTesting
	public SourceCounter getCounters(){
		return sourceCounter;
	}
}
