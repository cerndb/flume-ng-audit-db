/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.source.deserializer;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.DurablePositionTracker;
import org.apache.flume.serialization.PositionTracker;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.serialization.ResettableInputStream;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class RManDeserializerTest {

	@Test
	public void parse() throws IOException{
		
		File file = new File("src/test/resources/RManDeserializerTest.tmp");
		
		PrintWriter writer = new PrintWriter(file, "UTF-8");
		writer.println("[Wed Jan 27 18:29:03 CET 2016] level_EXEC_BACKUPSET_A edhp_rac51 itrac5105 EDHP1");
		writer.println("find: `/ORA/dbs01/syscontrol/local/logs/rman/level_arch0_newdisk.timtst_rac13.21012016_1827': No such file or dire");
		writer.println("[Wed Jan 27 18:29:03 CET 2016] level_EXEC_BACKUPSET_A edhp_rac51 itrac5105 EDHP1 removing logs : ");
		writer.println("rmanHost                = itrac5105");
		writer.println("parallelConfFile        = /ORA/dbs01/syscontrol/projects/rman/etc/rmanActionsArch01.conf");
		writer.println("rmanHost                = itrac5105");
		writer.println("tabxmlTsmServer         = TSM514_ORA");
		writer.println("remoteTsmServer         = TSM514_ORA");
		writer.println("tabxmlTDPONode          = edhp");
		writer.println("remoteTDPONode          = edhp_ora");
		writer.println(" RMAN-03009: failure of backup command on ORA_SBT_TAPE_1 channel at 01/27/2016 18:56:50");
		writer.println(" ORA-27028: skgfqcre: sbtbackup returned error");
		writer.println("ORA-19511: Error received from media manager layer, error text:");
		writer.println("    ANS1017E (RC-50)  Session rejected: TCP/IP connection failure.");
		writer.println("RMAN-00569: =============== ERROR MESSAGE STACK FOLLOWS ===============");
		writer.close();
	    
		File metaFile = new File("src/test/resources/RManDeserializerTest.metafile");
		
		PositionTracker tracker = DurablePositionTracker.getInstance(metaFile, file.getAbsolutePath());
		ResettableInputStream in = new ResettableFileInputStream(file, tracker);
		
		RManDeserializer des = (RManDeserializer) new RManDeserializer.Builder().build(new Context(), in);
	
		Event event = des.readEvent();
		JSONObject json = null;
		try {
			json = new JSONObject(new String(event.getBody()));
			
			Assert.assertEquals("2016-01-27T18:29:00+0100", json.get("startTimestamp"));
			Assert.assertEquals("level_EXEC_BACKUPSET_A", json.get("backupType"));
			Assert.assertEquals("edhp_rac51", json.get("entityName"));
			Assert.assertEquals("itrac5105", json.get("rmanHost"));
			Assert.assertEquals("/ORA/dbs01/syscontrol/projects/rman/etc/rmanActionsArch01.conf", 
					json.get("parallelConfFile"));
			Assert.assertEquals("TSM514_ORA", json.get("tabxmlTsmServer"));
			Assert.assertEquals("TSM514_ORA", json.get("remoteTsmServer"));
			Assert.assertEquals("edhp", json.get("tabxmlTDPONode"));
			Assert.assertEquals("edhp_ora", json.get("remoteTDPONode"));
		
		} catch (JSONException e) {
			e.printStackTrace();
			Assert.fail();
		}

		file.delete();
		metaFile.delete();
	}
	
	@Test
	public void parseFromSuccesfulFile() throws IOException{
		
		File file = new File("src/test/resources/rman-logs/level_arch_newdisk.edhp_rac51.05042016_0531");
		
		if(!file.exists())
			return;
		
		File metaFile = new File("src/test/resources/RManDeserializerTest.metafile");
		
		PositionTracker tracker = DurablePositionTracker.getInstance(metaFile, file.getAbsolutePath());
		ResettableInputStream in = new ResettableFileInputStream(file, tracker);
		
		RManDeserializer des = (RManDeserializer) new RManDeserializer.Builder().build(new Context(), in);
	
		Event event = des.readEvent();
		JSONObject json = null;
		try {
			json = new JSONObject(new String(event.getBody()));
			
			Assert.assertEquals("2016-04-05T05:31:00+0200", json.get("startTimestamp"));
			Assert.assertEquals("level_arch_newdisk", json.get("backupType"));
			Assert.assertEquals("edhp_rac51", json.get("entityName"));
			
		} catch (JSONException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		System.out.println(json);

		metaFile.delete();
	}
	
	@Test
	public void parseFromFailedFile() throws IOException{
		
		File file = new File("src/test/resources/rman-logs/level_EXEC_BACKUPSET_A.edhp_rac51.27012016_1829");
		
		if(!file.exists())
			return;
		
		File metaFile = new File("src/test/resources/RManDeserializerTest.metafile");
		
		PositionTracker tracker = DurablePositionTracker.getInstance(metaFile, file.getAbsolutePath());
		ResettableInputStream in = new ResettableFileInputStream(file, tracker);
		
		RManDeserializer des = (RManDeserializer) new RManDeserializer.Builder().build(new Context(), in);
	
		Event event = des.readEvent();
		JSONObject json = null;
		try {
			json = new JSONObject(new String(event.getBody()));
			
			Assert.assertEquals("2016-01-27T18:29:00+0100", json.get("startTimestamp"));
			Assert.assertEquals("level_EXEC_BACKUPSET_A", json.get("backupType"));
			Assert.assertEquals("edhp_rac51", json.get("entityName"));
			
		} catch (JSONException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		System.out.println(json);

		metaFile.delete();
	}

}
