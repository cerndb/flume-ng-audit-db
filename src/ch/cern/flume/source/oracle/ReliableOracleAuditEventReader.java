package ch.cern.flume.source.oracle;

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
import org.apache.flume.client.avro.ReliableEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReliableOracleAuditEventReader implements ReliableEventReader {

	private static final Logger LOG = LoggerFactory.getLogger(ReliableOracleAuditEventReader.class);
	
	private static final String CONNECTION_URL = "jdbc:oracle:oci8:@";
	
	private OracleDataSource dataSource = null;
	private Connection connection = null;
	private ResultSet resultSet = null;
	private Statement statement;
	
	private String last_timestamp = null;
	private String last_commited_timestamp = null;

	private int columnCount;

	private ArrayList<String> columnNames;

	private AuditEventDeserialezer deserializer;
	
	public ReliableOracleAuditEventReader(AuditEventDeserialezer deserializer) {
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
		if(this.deserializer instanceof AvroAuditEventDeserialezer)
			((AvroAuditEventDeserialezer) this.deserializer).setFieldNames(columnNames);
		
		loadLastCommitedTimestamp();
	}
	
	private List<String> getColumnNames() {
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
		}
		
		return null;
	}

	private void loadLastCommitedTimestamp() {
		// TODO Auto-generated method stub
		last_commited_timestamp = null;
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
						event.addField(columnNames.get(i), value);
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
		//commit last_timestamp to file
		
		
		if(last_timestamp != null)
			last_commited_timestamp = last_timestamp;
		last_timestamp = null;
	}

	@Override
	public void close() throws IOException {
		try {
			connection.close();
			statement.close();
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

}
