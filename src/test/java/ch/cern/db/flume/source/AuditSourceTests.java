package ch.cern.db.flume.source;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.PollableSourceRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ch.cern.db.flume.source.AuditSource;
import ch.cern.db.flume.source.reader.ReliableJdbcAuditEventReader;

public class AuditSourceTests{

	String connection_url = "jdbc:hsqldb:mem:aname";
	Connection connection = null;
	
	@Before
	public void setup(){
		try {
			connection = DriverManager.getConnection(connection_url, "sa", "");
			
			Statement statement = connection.createStatement();
			statement.execute("DROP TABLE IF EXISTS audit_data_table;");
			statement.execute("CREATE TABLE audit_data_table (id INTEGER, return_code BIGINT, name VARCHAR(20));");
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}
	
	@Test
	public void basic() throws SQLException, InterruptedException, EventDeliveryException, IOException{
		
		Statement statement = connection.createStatement();
		statement.execute("INSERT INTO audit_data_table VALUES 1, 48, 'name1';");
		statement.execute("INSERT INTO audit_data_table VALUES 3, 48, 'name3';");
		statement.close();
		
		Context context = new Context();
		context.put(ReliableJdbcAuditEventReader.CONNECTION_DRIVER_PARAM, "org.hsqldb.jdbc.JDBCDriver");
		context.put(ReliableJdbcAuditEventReader.CONNECTION_URL_PARAM, connection_url);
		context.put(ReliableJdbcAuditEventReader.USERNAME_PARAM, "SA");
		context.put(ReliableJdbcAuditEventReader.PASSWORD_PARAM, "");
		context.put(ReliableJdbcAuditEventReader.TABLE_NAME_PARAM, " audit_data_table");
		context.put(ReliableJdbcAuditEventReader.COLUMN_TO_COMMIT_PARAM, "ID");
		context.put(ReliableJdbcAuditEventReader.TYPE_COLUMN_TO_COMMIT_PARAM, "numeric");
		
		AuditSource source = new AuditSource();
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
	    
	    Thread.sleep(1000);
	    
	    channel.getTransaction().begin();
	    
	    Event event = channel.take();
	    Assert.assertEquals("{\"ID\":1,\"RETURN_CODE\":48,\"NAME\":\"name1\"}", new String(event.getBody()));
	    event = channel.take();
	    Assert.assertEquals("{\"ID\":3,\"RETURN_CODE\":48,\"NAME\":\"name3\"}", new String(event.getBody()));
	    event = channel.take();
	    Assert.assertNull(event);
	    
	    channel.getTransaction().commit();
	    channel.getTransaction().close();;
	    
	    runner.stop();
	    
	    //Check content of committing file 
	    FileReader in = new FileReader(ReliableJdbcAuditEventReader.COMMITTING_FILE_PATH_DEFAULT);
		char [] in_chars = new char[50];
	    in.read(in_chars);
		in.close();
		String committed_value_from_file = new String(in_chars).trim();
		Assert.assertEquals("3", committed_value_from_file);
		
	}

	@After
	public void cleanUp(){
		new File(ReliableJdbcAuditEventReader.COMMITTING_FILE_PATH_DEFAULT).delete();
		
		try {
			connection.close();
		} catch (SQLException e) {}
	}
}
