package ch.cern.db.flume.sink.kite.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class InferSchemaFromTable {
	
	private static boolean showHelp;

	private Options options;
	
	private static final String DRIVER_CLASS_DEFAULT = "oracle.jdbc.driver.OracleDriver";

	private String connection_url;
	private String connection_user;
	private String connection_password;

	private String tableName;
	private String tableCatalog;
	private String tableSchema;

	public InferSchemaFromTable() {
		options = new Options();
		options.addOption(Option.builder("p")
				.desc("User's password")
				.hasArg()
				.argName("PASSWORD")
				.required()
				.build());
		options.addOption(Option.builder("u")
				.desc("User to authenticate against database")
				.hasArg()
				.argName("USERNSME")
				.required()
				.build());
		options.addOption(Option.builder("t")
				.desc("Table from which schema is inferred")
				.hasArg()
				.argName("TABLE_NAME")
				.required()
				.build());
		options.addOption(Option.builder("c")
				.desc("URL for connecting to database")
				.hasArg()
				.argName("CONNECTION_URL")
				.required()
				.build());
		options.addOption(Option.builder("dc")
				.desc("Fully qualified class name of JDBC driver (default: "+DRIVER_CLASS_DEFAULT+")")
				.hasArg()
				.argName("DRIVER_FQCN")
				.build());
		options.addOption(Option.builder("catalog")
				.desc("Catalog table")
				.hasArg()
				.argName("CATALOG_NAME")
				.build());
		options.addOption(Option.builder("schema")
				.desc("Table schema")
				.hasArg()
				.argName("SCHEMA_NAME")
				.build());
		options.addOption(Option.builder("help")
				.desc("Print help")
				.build());
	}
	
	public void configure(String[] args){
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options , args);
			
			showHelp = cmd.hasOption("help");
		} catch (ParseException e) {
			System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
			printHelp();
			System.exit(1);
		}
		
		String driverClass = cmd.getOptionValue("dc", DRIVER_CLASS_DEFAULT);
		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(2);
		}
		
		connection_url = cmd.getOptionValue("c");
		connection_user = cmd.getOptionValue("u");
		connection_password = cmd.getOptionValue("p");
		tableName = cmd.getOptionValue("t");
		tableSchema = cmd.getOptionValue("schema");
		tableCatalog = cmd.getOptionValue("catalog");
	}
	
	public static void main(String[] args) {
		InferSchemaFromTable schemaGenerator = new InferSchemaFromTable();
		schemaGenerator.configure(args);
		
		if(showHelp){
			schemaGenerator.printHelp();
			return;
		}
		
		Schema schema = null;
		try {
			schema = schemaGenerator.getSchema();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		System.out.println(schema.toString(true));
	}

	public Schema getSchema() throws SQLException {

		FieldAssembler<Schema> builder = SchemaBuilder.record("audit").fields();
		
		Connection connection = DriverManager.getConnection(
				connection_url, 
				connection_user, 
				connection_password);

		ResultSet result = connection.getMetaData().getColumns(tableCatalog, tableSchema, tableName, null);
		boolean tableFound = false;
		while(result.next()){
			tableFound = true;
			
		    String columnName = result.getString(4);
		    int    columnType = result.getInt(5);
		    
		    switch (columnType) {
			case java.sql.Types.SMALLINT:
			case java.sql.Types.TINYINT:
			case java.sql.Types.INTEGER:
			case java.sql.Types.BIGINT:
				builder.name(columnName).type().nullable().intType().noDefault();
				break;
			case java.sql.Types.BOOLEAN:
				builder.name(columnName).type().nullable().booleanType().noDefault();
				break;
			case java.sql.Types.NUMERIC:
			case java.sql.Types.DOUBLE:
			case java.sql.Types.FLOAT:
				builder.name(columnName).type().nullable().doubleType().noDefault();
				break;
			case java.sql.Types.TIMESTAMP:
			case -102: //TIMESTAMP(6) WITH LOCAL TIME ZONE
				builder.name(columnName).type().nullable().longType().noDefault();
				break;
			default:
				builder.name(columnName).type().nullable().stringType().noDefault();
				break;
			}
		}
		
		if(!tableFound)
			throw new SQLException("A table with configured specifications ("
					+ "catalog=" + tableCatalog
					+ ", schema=" + tableSchema
					+ ", name=" + tableName
					+ ") could not be found");
		
		return builder.endRecord();
	}

	public void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setOptionComparator(new Comparator<Option>() {
			@Override
			public int compare(Option o1, Option o2) {
				if(o1.isRequired())
					return -1;
				if(o2.isRequired())
					return 1;
				
				return 0;
			}
		});
		formatter.setWidth(150);
		
		formatter.printHelp("./infer-avro-schema-from-database.sh", 
				options,
				true);
	}
	
}
