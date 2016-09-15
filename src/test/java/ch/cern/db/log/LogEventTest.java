package ch.cern.db.log;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

public class LogEventTest {
	
	@Test
	public void listenerLogsExtractTimestamp(){
		
		DateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyy HH:mm:ss");

		String line = "Fri Jul 29 15:27:34 2016";
		
		try {
			LogEvent.extractTimestamp(dateFormat, line);
			
			Assert.fail();
		} catch (ParseException e) {
		}
		
		line = "29-JUL-2016 15:17:34 "
				+ "* (CONNECT_DATA=(SID=DESFOUND)(CID=(PROGRAM=oracle)(HOST=itrac50035.cern.ch)(USER=oracle))) "
				+ "* (ADDRESS=(PROTOCOL=tcp)(HOST=137.138.147.42)(PORT=30816)) "
				+ "* establish "
				+ "* DESFOUND "
				+ "* 0";
		try {
			Date timestamp = LogEvent.extractTimestamp(dateFormat, line);
			
			Assert.assertEquals(dateFormat.parse("29-JUL-2016 15:17:34"), timestamp);
		} catch (ParseException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		line = "12-FEB-2014 07:11:30 * service_update * WCERND * 0";
		try {
			Date timestamp = LogEvent.extractTimestamp(dateFormat, line);
			
			Assert.assertEquals(dateFormat.parse("12-FEB-2014 07:11:30"), timestamp);
		} catch (ParseException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void log4jLogsExtractTimestamp(){
		DateFormat dateFormat = new SimpleDateFormat("dd MMM yyyy HH:mm:ss,SSS");

		String line = "Fri Jul 29 15:27:34 2016";
		
		try {
			LogEvent.extractTimestamp(dateFormat, line);
			
			Assert.fail();
		} catch (ParseException e) {
		}
		
		line = "01 Aug 2016 11:18:17,571 "
				+ "INFO  "
				+ "[SinkRunner-PollingRunner-DefaultSinkProcessor] "
				+ "(com.frontier45.flume.sink.elasticsearch2.client.ElasticSearchRestClient.execute:133)  "
				+ "- Status code from elasticsearch: 200";
		try {
			Date timestamp = LogEvent.extractTimestamp(dateFormat, line);
			
			Assert.assertEquals(dateFormat.parse("01 Aug 2016 11:18:17,571"), timestamp);
		} catch (ParseException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		line = "31 Jul 2016 06:11:54,663 "
				+ "INFO  "
				+ "[SinkRunner-PollingRunner-DefaultSinkProcessor] "
				+ "(org.apache.flume.sink.kite.DatasetSink.closeWriter:483)  "
				+ "- Closed writer for dataset:hdfs://p01001532067275.cern.ch:8020/user/dblogs/audit-11/ "
				+ "after 305 seconds and 274848 bytes parsed";
		try {
			Date timestamp = LogEvent.extractTimestamp(dateFormat, line);
			
			Assert.assertEquals(dateFormat.parse("31 Jul 2016 06:11:54,663"), timestamp);
		} catch (ParseException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
}
