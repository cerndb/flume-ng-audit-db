package ch.cern.db.flume.source.reader;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.db.flume.JSONEvent;

public class ReliableJdbcEventReader implements Configurable{

	private static final Logger LOG = LoggerFactory.getLogger(ReliableJdbcEventReader.class);
	
	enum State{INITIALIZED, CONFIGURED};
	private State state;
	
	public static final String CONNECTION_DRIVER_DEFAULT = "oracle.jdbc.driver.OracleDriver";
	public static final String CONNECTION_DRIVER_PARAM = "reader.connectionDriver";
	private String connection_driver = CONNECTION_DRIVER_DEFAULT;
	
	public static final String CONNECTION_URL_DEFAULT = "jdbc:oracle:oci:@";
	public static final String CONNECTION_URL_PARAM = "reader.connectionUrl";
	private String connection_url = CONNECTION_URL_DEFAULT;
	
	public static final String USERNAME_PARAM = "reader.username";
	public static final String USERNAME_DEFAULT = "sys as sysdba";
	private String connection_user = USERNAME_DEFAULT;;
	
	public static final String PASSWORD_PARAM = "reader.password";
	public static final String PASSWORD_DEFAULT = "sys";
	private String connection_password = PASSWORD_DEFAULT;
	
	public static final String TABLE_NAME_PARAM = "reader.table";
	private String tableName = null;
	
	public static final String COLUMN_TO_COMMIT_PARAM = "reader.table.columnToCommit";
	private String columnToCommit = null;
	protected String committed_value = null;
	
	enum ColumnType {STRING, TIMESTAMP, NUMERIC}
	public static final String TYPE_COLUMN_TO_COMMIT_PARAM = "reader.table.columnToCommit.type";
	public static final ColumnType TYPE_COLUMN_TO_COMMIT_DEFUALT = ColumnType.TIMESTAMP;
	private ColumnType type_column_to_commit = TYPE_COLUMN_TO_COMMIT_DEFUALT;

	public static final String COMMITTED_VALUE_TO_LOAD_PARAM = "reader.committtedValue";
	private String committed_value_to_load = null;
	
	public static final String QUERY_PARAM = "reader.query";
	public static final String QUERY_PATH_PARAM = "reader.query.path";
	private String configuredQuery = null;
	
	public static final String COMMITTING_FILE_PATH_DEFAULT = "committed_value.backup";
	public static final String COMMITTING_FILE_PATH_PARAM = "reader.committingFile";
	private String committing_file_path = COMMITTING_FILE_PATH_DEFAULT;
	private File committing_file = null;
	
	private Connection connection = null;
	private ResultSet resultSet = null;
	private Statement statement = null;
	
	protected String last_value = null;
	
	public ReliableJdbcEventReader() {
		initialize();
	}
	
	@Override
	public void configure(Context context) {
		initialize();
		
		tableName = context.getString(TABLE_NAME_PARAM);
		configuredQuery = getConfiguredQuery(context.getString(QUERY_PARAM), context.getString(QUERY_PATH_PARAM));
		if(configuredQuery == null && tableName == null)
			throw new ConfigurationException("Table name or query needs to be configured with " + TABLE_NAME_PARAM);
		
		columnToCommit = context.getString(COLUMN_TO_COMMIT_PARAM);
		if(columnToCommit == null)
			throw new ConfigurationException("Column to commit needs to be configured with " + COLUMN_TO_COMMIT_PARAM);
		
		String conf_type_column = context.getString(TYPE_COLUMN_TO_COMMIT_PARAM);
		if(conf_type_column != null){
			try{
				type_column_to_commit = ColumnType.valueOf(conf_type_column.toUpperCase());
			}catch(Exception e){
				throw new FlumeException("Configuration value for " + TYPE_COLUMN_TO_COMMIT_PARAM
						+ " is not valid, it must be one of: " + Arrays.asList(ColumnType.values()));
			}
		}
		
		connection_driver = context.getString(CONNECTION_DRIVER_PARAM, CONNECTION_DRIVER_DEFAULT);
		try {
			Class.forName(connection_driver);
		} catch (ClassNotFoundException e) {
			throw new FlumeException("Configured class for JDBC driver ("
					+ connection_driver + ") has not been found in the classpath");
		}
		
		connection_user = context.getString(USERNAME_PARAM, USERNAME_DEFAULT);
		connection_password = context.getString(PASSWORD_PARAM, PASSWORD_DEFAULT);
		connection_url = context.getString(CONNECTION_URL_PARAM, CONNECTION_URL_DEFAULT);
		
		committing_file_path = context.getString(COMMITTING_FILE_PATH_PARAM, COMMITTING_FILE_PATH_DEFAULT);
		committing_file = new File(committing_file_path);
		
		loadLastCommittedValueFromFile();
		if(committed_value == null){
			committed_value_to_load = context.getString(COMMITTED_VALUE_TO_LOAD_PARAM);
			committed_value = committed_value_to_load;
		}
		
		state = State.CONFIGURED;
	}

	private void initialize() {
		try{			
			if(resultSet != null){
				resultSet.close();
				resultSet = null;
			}
			if(statement != null){
				statement.close();
				statement = null;
			}
			if(connection != null){
				connection.close();
				connection = null;
			}
		} catch (Exception e) {
			LOG.warn(e.getMessage());
		}
		
		last_value = null;
		
		state = State.INITIALIZED;
	}

	private String getConfiguredQuery(String query_string, String path_to_query_file) {
		if(state != State.INITIALIZED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not initialized");
		
		//From configuration parameter
		if(query_string != null)
			return query_string;
		
		//Else, from file if path is configured
		if(path_to_query_file == null)
			return null;
		
		File query_file = new File(path_to_query_file); 
		if(query_file.exists()){
			try {
				FileReader in = new FileReader(query_file);
				
				char [] in_chars = new char[(int) query_file.length()];
			    in.read(in_chars);
				in.close();
				
				return new String(in_chars).trim();
			} catch (Exception e) {
				throw new FlumeException(e);
			}
		}else{
			throw new FlumeException("File configured with "+QUERY_PATH_PARAM+" parameter does not exist");
		}
	}

	private void loadLastCommittedValueFromFile() {
		if(state != State.INITIALIZED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not initialized");
		
		try {
			if(committing_file.exists()){
				FileReader in = new FileReader(committing_file);
				char [] in_chars = new char[(int) committing_file.length()];
			    in.read(in_chars);
				in.close();
				String value_from_file = new String(in_chars).trim();
				
				if(value_from_file.length() > 0){
					committed_value = value_from_file;
					
					LOG.info("Last value loaded from file: " + committed_value);
				}else{
					LOG.info("File for loading last value is empty");
				}
			}else{
				committing_file.createNewFile();
				
				LOG.info("File for storing last commited value has been created: " +
						committing_file.getAbsolutePath());
			}
		} catch (IOException e) {
			throw new FlumeException(e);
		}
	}

	public Event readEvent() throws IOException {
		if(state != State.CONFIGURED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not configured");
		
		try {
			if(resultSet == null)
				runQuery();
			
			if(resultSet != null && !resultSet.isClosed() && resultSet.next()){
				ResultSetMetaData metadata = resultSet.getMetaData();
				int columnCount = metadata.getColumnCount();
				
				JSONEvent event = new JSONEvent();
				
				for (int i = 1; i <= columnCount; i++) {										
					String name = metadata.getColumnName(i);
					
					switch (metadata.getColumnType(i)) {
					case java.sql.Types.SMALLINT:
					case java.sql.Types.TINYINT:
					case java.sql.Types.INTEGER:
					case java.sql.Types.BIGINT:
						event.addProperty(name, resultSet.getInt(i));
						break;
					case java.sql.Types.BOOLEAN:
						event.addProperty(name, resultSet.getBoolean(i));
						break;
					case java.sql.Types.NUMERIC:
					case java.sql.Types.DOUBLE:
					case java.sql.Types.FLOAT:
						event.addProperty(name, resultSet.getDouble(i));
						break;
					case java.sql.Types.TIMESTAMP:
					case -102: //TIMESTAMP(6) WITH LOCAL TIME ZONE
						String ts = resultSet.getTimestamp(i).toString();
						ts = ts.substring(0, 23).replace(" ", "T");
						event.addProperty(name, ts);
						break;
					default:
						event.addProperty(name, resultSet.getString(i));
						break;
					}
					
					if(name.equals(columnToCommit)){
						last_value = resultSet.getString(i);
					}
				}				
				
				return event;
			}else{
				resultSet = null;
				
				return null;
			}
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	private void runQuery() throws SQLException {
		if(state != State.CONFIGURED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not configured");
		
		if(statement != null)
			statement.close();
		
		connect();
		
		statement = connection.createStatement();
		
		String query = createQuery(committed_value);
		
		LOG.info("Executing query: " + query);
		
		resultSet = statement.executeQuery(query);
	}

	protected String createQuery(String committed_value) {
		if(state != State.CONFIGURED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not configured");
		
		if(configuredQuery != null){
			String finalQuery = configuredQuery;
			
			if(committed_value == null){
				//Remove query between [] characters
				while(true){
					int left_index = finalQuery.indexOf('[');
					int right_index = finalQuery.indexOf(']');
				
					if(left_index == -1 || right_index == -1)
						break;
					
					finalQuery = finalQuery.substring(0, left_index)
						.concat(finalQuery.substring(right_index+1, finalQuery.length()));
				}
			}else{
				String toReplace = ":committed_value";
				
				//Replace all {$committed_value} by committed_value
				finalQuery = finalQuery.replaceAll(toReplace, committed_value);
				
				//Remove [] characters
				finalQuery = finalQuery.replaceAll("\\[", " ").replaceAll("\\]", " ");
			}
			
			return finalQuery;
		}
		
		String query = "SELECT * FROM " + tableName;
		
		if(committed_value != null){
			query = query.concat(" WHERE " + columnToCommit + " > ");
		
			switch (type_column_to_commit) {
			case NUMERIC:
				query = query.concat(committed_value);
				break;
			case TIMESTAMP:
				query = query.concat("TIMESTAMP \'" + committed_value + "\'");
				break;
			default: //String
				query = query.concat("\'" + committed_value + "\'");
				break;
			}
		}
		
		query = query.concat(" ORDER BY " + columnToCommit);
		
		return query;
	}

	private void connect() throws SQLException{
		if(state != State.CONFIGURED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not configured");
		
		try {
			if(connection == null || connection.isClosed()){
				connection = DriverManager.getConnection(
						connection_url, 
						connection_user, 
						connection_password);
			}
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
			throw e;
		}
	}
	
	public List<Event> readEvents(int numberOfEventToRead) throws IOException {
		if(state != State.CONFIGURED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not configured");
		
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

	public void commit() throws IOException {
		if(state != State.CONFIGURED)
			throw new ConfigurationException(getClass().getSimpleName() + " is not configured");
		
		if(last_value == null)
			return;
		
		committed_value = last_value;
		
		FileWriter out = new FileWriter(committing_file, false);
		out.write(committed_value);
		out.close();
		
		last_value = null;
	}

	public void close() throws IOException {
		try {
			if(statement != null)
				statement.close();
			if(connection != null)
				connection.close();
		} catch (Throwable e) {
		}
	}

}
