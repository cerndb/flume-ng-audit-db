package ch.cern.db.audit.flume.source.reader;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import oracle.jdbc.driver.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.client.avro.ReliableEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import ch.cern.db.audit.flume.AuditEvent;
import ch.cern.db.audit.flume.source.deserializer.AuditEventDeserializer;

public class ReliableOracleAuditEventReader implements ReliableEventReader {

	private static final Logger LOG = LoggerFactory.getLogger(ReliableOracleAuditEventReader.class);
	
	private static final String CONNECTION_URL = "jdbc:oracle:oci:@";

	public static final String TIMESTAMP_FILE_PATH = "last_commited_timestamp.backup";
	
	private OracleDataSource dataSource = null;
	private Connection connection = null;
	private ResultSet resultSet = null;
	private Statement statement = null;
	
	protected String last_timestamp = null;
	protected String last_commited_timestamp = null;
	private File last_commited_timestamp_file = null;
	
	private int columnCount;

	private ArrayList<String> columnNames;

	private AuditEventDeserializer deserializer;
	
	@VisibleForTesting
	protected ReliableOracleAuditEventReader(){
		loadLastCommitedTimestamp();
	}
	
	public ReliableOracleAuditEventReader(AuditEventDeserializer deserializer) {
		Properties prop = new Properties();
		prop.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, "sys");
		prop.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, "sys");
		prop.put(OracleConnection.CONNECTION_PROPERTY_INTERNAL_LOGON, "sysdba");
		
		try {
			dataSource = new OracleDataSource();
			dataSource.setConnectionProperties(prop);
			dataSource.setURL(CONNECTION_URL);
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
		}
		
		columnNames = (ArrayList<String>) getColumnNames();
		
		this.deserializer = deserializer;
		
		loadLastCommitedTimestamp();
	}

	protected List<String> getColumnNames() {
		try {
			connect();
			
			Statement statement = connection.createStatement();
			String query = "SELECT * FROM UNIFIED_AUDIT_TRAIL WHERE ROWNUM < 1";
			ResultSet resultSet = statement.executeQuery(query);
			ResultSetMetaData metadata = resultSet.getMetaData();
			columnCount = metadata.getColumnCount();
			
			List<String> columnNames = new ArrayList<String>();			
			for (int i = 1; i <= columnCount; i++){
				columnNames.add(metadata.getColumnName(i));
			}

			return columnNames;
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
			
			throw new FlumeException(e);
		}
	}

	private void loadLastCommitedTimestamp() {
		try {
			last_commited_timestamp_file = new File(TIMESTAMP_FILE_PATH);
			
			if(last_commited_timestamp_file.exists()){
				FileReader in = new FileReader(last_commited_timestamp_file);
				char [] in_chars = new char[60];
			    in.read(in_chars);
				in.close();
				String timestamp_from_file = new String(in_chars).trim();
				
				if(timestamp_from_file.length() > 1){
					last_commited_timestamp = timestamp_from_file;
					
					LOG.info("Last timestamp loaded from file: " + last_commited_timestamp);
				}else{
					LOG.info("File for loading last timestamp is empty");
				}
			}else{
				last_commited_timestamp_file.createNewFile();
				
				LOG.info("File for storing last commited timestamp have been created: " +
						TIMESTAMP_FILE_PATH);
			}
		} catch (IOException e) {
			throw new FlumeException(e);
		}
	}

	@Override
	public Event readEvent() throws IOException {
		try {
			if(resultSet == null)
				runQuery();
			
			if(resultSet != null && !resultSet.isClosed() && resultSet.next()){
				AuditEvent event = new AuditEvent();
				
				for (int i = 1; i <= columnCount; i++) {					
					String value = resultSet.getString(i);
					
					if(value != null)
						event.addField(columnNames.get(i - 1), value);
				}				

				last_timestamp = resultSet.getString(20);
				
				return deserializer.process(event);
			}else{
				resultSet = null;
				
				return null;
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	private void runQuery() throws SQLException {	
		if(statement != null)
			statement.close();
		
		connect();
		
		statement = connection.createStatement();
		String query = "SELECT * " +
				"FROM UNIFIED_AUDIT_TRAIL " +
				(last_commited_timestamp == null ? "" : "WHERE EVENT_TIMESTAMP > TIMESTAMP \'" + last_commited_timestamp + "\'") +
				"ORDER BY EVENT_TIMESTAMP";
		
		resultSet = statement.executeQuery(query);
		
		LOG.info("Executing query: " + query);
	}

	private void connect() throws SQLException{
		try {
			if(connection == null || connection.isClosed())
				connection = dataSource.getConnection();
			
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
			throw e;
		}
	}
	
	@Override
	public List<Event> readEvents(int numberOfEventToRead) throws IOException {
		LinkedList<Event> events = new LinkedList<Event>();
		
		for (int i = 0; i < numberOfEventToRead; i++){
			Event event = readEvent();
			
			if(event != null)
				events.add(event);
			else{
				LOG.info("Number of events returned: " + events.size());
				return events;
			}
		}
		
		LOG.info("Number of events returned: " + events.size());
		return events;
	}

	@Override
	public void commit() throws IOException {
		if(last_timestamp == null)
			return;
		
		last_commited_timestamp = last_timestamp;
		
		FileWriter out = new FileWriter(last_commited_timestamp_file, false);
		out.write(last_commited_timestamp);
		out.close();
		
		last_timestamp = null;
	}

	@Override
	public void close() throws IOException {
		try {
			connection.close();
			statement.close();
		} catch (Throwable e) {
		}
	}

}
