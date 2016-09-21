/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.source;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.PollableSourceRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.flume.source.reader.ReliableJdbcEventReader;
import ch.cern.db.flume.source.reader.ReliableLogFileEventReader;
import ch.cern.db.flume.source.reader.log.DefaultLogEventParser;

public class LogFileSourceTest extends Assert{
	
	private File logFile = new File("src/test/resources/sample-logs/listener.log");

	@Test
	public void basic() throws InterruptedException {
		
		Context context = new Context();
		context.put(ReliableLogFileEventReader.LOG_FILE_PATH_PARAM, logFile.getAbsolutePath());
		context.put(ReliableLogFileEventReader.DATAFORMAT_PARAM, "dd-MMM-yyy HH:mm:ss");
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");

		LogFileSource source = new LogFileSource();
		source.configure(context);
		
		Map<String, String> channelContext = new HashMap<String, String>();
	    channelContext.put("capacity", "100");
	    channelContext.put("keep-alive", "0"); // for faster tests
	    Channel channel = new MemoryChannel();
	    Configurables.configure(channel, new Context(channelContext));
	    
	    ChannelSelector rcs = new ReplicatingChannelSelector();
	    rcs.setChannels(Collections.singletonList(channel));
	    ChannelProcessor chp = new ChannelProcessor(rcs);
	    source.setChannelProcessor(chp);
	    
	    PollableSourceRunner runner = new PollableSourceRunner();
	    runner.setSource(source);
	    runner.start();
	    
	    Thread.sleep(100);
	    
	    channel.getTransaction().begin();
	    	    
	    // Read a few events
	    Event event = channel.take();
  		Assert.assertEquals("2016-07-29T15:17:34+0200", event.getHeaders().get(
  				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
  		event = channel.take();
  		Assert.assertEquals("2016-07-29T15:17:38+0200", event.getHeaders().get(
  				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
  		event = channel.take();
  		Assert.assertEquals("2016-07-29T15:18:45+0200", event.getHeaders().get(
  				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
  		event = channel.take();
  		Assert.assertEquals("2016-07-29T15:19:55+0200", event.getHeaders().get(
  				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
  		event = channel.take();
  		Assert.assertEquals("2016-07-29T15:19:56+0200", event.getHeaders().get(
  				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
	    
	    channel.getTransaction().commit();
	    channel.getTransaction().close();
	    
	    runner.stop();
	}
	
	@Test
	public void rollBack() throws InterruptedException {
		
		Context context = new Context();
		context.put(ReliableLogFileEventReader.LOG_FILE_PATH_PARAM, logFile.getAbsolutePath());
		context.put(ReliableLogFileEventReader.DATAFORMAT_PARAM, "dd-MMM-yyy HH:mm:ss");
		context.put(ReliableJdbcEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(LogFileSource.BATCH_SIZE_PARAM, "2");
		context.put(LogFileSource.MINIMUM_BATCH_TIME_PARAM, "100");
		
		LogFileSource source = new LogFileSource();
		source.configure(context);
		
		Map<String, String> channelContext = new HashMap<String, String>();
	    channelContext.put("capacity", "2");
	    channelContext.put("transactionCapacity", "2");
	    channelContext.put("keep-alive", "0"); // for faster tests
	    Channel channel = new MemoryChannel();
	    Configurables.configure(channel, new Context(channelContext));
	    
	    ChannelSelector rcs = new ReplicatingChannelSelector();
	    rcs.setChannels(Collections.singletonList(channel));
	    ChannelProcessor chp = new ChannelProcessor(rcs);
	    source.setChannelProcessor(chp);
	    
	    PollableSourceRunner runner = new PollableSourceRunner();
	    runner.setSource(source);
	    runner.start();
	    
	    Thread.sleep(500);
	    
	    // During first transaction source will try to put events but will get:
	    // org.apache.flume.ChannelFullException: Space for commit to queue couldn't be acquired...
	    // Because channel capacity is not enough
	    // That will cause roll backs
	    
	    channel.getTransaction().begin();
	    
	    Event event = channel.take();
  		Assert.assertEquals("2016-07-29T15:17:34+0200", event.getHeaders().get(
  				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
  		event = channel.take();
  		Assert.assertEquals("2016-07-29T15:17:38+0200", event.getHeaders().get(
  				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
  		
	    channel.getTransaction().commit();
	    channel.getTransaction().close();
	    
	    Thread.sleep(200);
	    
	    // Once channel is empty, third event should be taken
	    
	    channel.getTransaction().begin();
	    
  		event = channel.take();
  		Assert.assertEquals("2016-07-29T15:18:45+0200", event.getHeaders().get(
  				DefaultLogEventParser.TIMESTAMP_HEADER_NAME));
	    
	    channel.getTransaction().commit();
	    channel.getTransaction().close();
	    
	    runner.stop();
	}
	
	@After
	public void cleanUp(){
		new File(ReliableLogFileEventReader.COMMITTING_FILE_PATH_DEFAULT).delete();
		new File(DropDuplicatedEventsProcessor.PATH_DEFAULT).delete();
	}
	
}
