package ch.cern.flume.source.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import oracle.jdbc.driver.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;

public class OracleAuditClient {

	private static final String CONNECTION_URL = "jdbc:oracle:oci8:@";
	
	

	public static void main(String[] args) throws SQLException {
		
		Properties prop = new Properties();
		prop.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, "sys");
		prop.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, "sys");
		prop.put(OracleConnection.CONNECTION_PROPERTY_INTERNAL_LOGON, "sysdba");
		
		OracleDataSource dataSource = new OracleDataSource();
		dataSource.setConnectionProperties(prop);
		dataSource.setURL(CONNECTION_URL);
		
		Connection connection = dataSource.getConnection();
		
		String last_timestamp = null;
		
		Statement statement = connection.createStatement();
		String query = "SELECT * " +
				"FROM UNIFIED_AUDIT_TRAIL " +
				"WHERE ROWNUM < 1";
		ResultSet resultSet = statement.executeQuery(query);
		ResultSetMetaData metadata = resultSet.getMetaData();
		int columnCount = metadata.getColumnCount();
		for (int i = 1; i <= columnCount; i++) {
			System.out.print(i + "-" + metadata.getColumnName(i) + "\t");
		}
		System.out.println();
		
		while (true) {
			last_timestamp = loadFromTable(connection, last_timestamp);
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}
		}
		
		//connection.close();
	}

	private static String loadFromTable(Connection connection, String last_timestamp) throws SQLException {
		Statement statement = connection.createStatement();
		String query = "SELECT * " +
				"FROM UNIFIED_AUDIT_TRAIL " +
				(last_timestamp == null ? "" : "WHERE EVENT_TIMESTAMP > TIMESTAMP \'" + last_timestamp + "\'") +
				"ORDER BY EVENT_TIMESTAMP";
		ResultSet resultSet = statement.executeQuery(query);
		ResultSetMetaData metadata = resultSet.getMetaData();
		int columnCount = metadata.getColumnCount();
		
		while(resultSet.next()){
			for (int i = 1; i <= columnCount; i++) {
				System.out.print(resultSet.getString(i) + "\t");
			}
			System.out.println();

			last_timestamp = resultSet.getString(20);
		}
		
		resultSet.close();
		
		return last_timestamp;
	}
	
	
}
