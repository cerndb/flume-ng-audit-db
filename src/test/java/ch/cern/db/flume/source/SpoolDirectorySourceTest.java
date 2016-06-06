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
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SpoolDirectorySourceTest{

	private File spoolDir;

	@Before
	public void setup(){
		spoolDir = new File("src/test/resources/rman-logs-tmp");
	    spoolDir.mkdirs();
	}
	
	@Test
	public void readWhenClosed() throws SQLException, InterruptedException, EventDeliveryException, IOException{
		
		Context context = new Context();
		context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY, spoolDir.getAbsolutePath());
		
		SpoolDirectorySource source = new SpoolDirectorySource();
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
	  
	    source.start();
	    
	    //Create file and close it
	    PrintWriter file1 = new PrintWriter(new File(spoolDir, "file1"), "UTF-8");
	    file1.println("file1-line1");
	    file1.println("file1-line2");
	    file1.close();
	    
	    Thread.sleep(2000);
	    
	    channel.getTransaction().begin();
	    
	    Event event = channel.take();
	    Assert.assertNotNull(event);
	    Assert.assertEquals("file1-line1", new String(event.getBody()));
	    event = channel.take();
	    Assert.assertNotNull(event);
	    Assert.assertEquals("file1-line2", new String(event.getBody()));
	    event = channel.take();
	    Assert.assertNull(event);
	    
	    channel.getTransaction().commit();
	    channel.getTransaction().close();
	    
	    
	    //Create file and leave it open
	    PrintWriter file2 = new PrintWriter(new File(spoolDir, "file2"), "UTF-8");
	    file2.println("file2-line1");
	    file2.println("file2-line2");
	    
	    Thread.sleep(2000);
	    
	    channel.getTransaction().begin();
	    event = channel.take();
	    Assert.assertNull(event);
	    channel.getTransaction().commit();
	    channel.getTransaction().close();
	    
	    //Once file is closed, it must be processed
	    file2.close();
	    file2.flush();
	    
	    Thread.sleep(2000);
	    
	    channel.getTransaction().begin();
	    
	    event = channel.take();
	    Assert.assertNotNull(event);
	    Assert.assertEquals("file2-line1", new String(event.getBody()));
	    event = channel.take();
	    Assert.assertNotNull(event);
	    Assert.assertEquals("file2-line2", new String(event.getBody()));
	    event = channel.take();
	    Assert.assertNull(event);
	    
	    channel.getTransaction().commit();
	    channel.getTransaction().close();
	    
	    source.stop();
	}

	@After
	public void cleanUp(){
		try {
			FileUtils.deleteDirectory(spoolDir);
		} catch (IOException e) {
		}
	}
}
