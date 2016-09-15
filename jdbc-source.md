# JDBCSource

In order to use JDBCSource as source in your Flume agent, you need to specify the type of agent source as:

```
<agent_name>.sources.<source_name>.type = ch.cern.db.flume.source.JDBCSource 
```

NOTE: you must add the JDBC driver library to Flume's classpath when running Flume agent.

Find below all available configuration parameters:

```
<agent_name>.sources.<source_name>.batch.size = 100
<agent_name>.sources.<source_name>.batch.minimumTime = 10000
<agent_name>.sources.<source_name>.reader.committingFile = committed_value.backup
<agent_name>.sources.<source_name>.reader.committtedValue = NULL
<agent_name>.sources.<source_name>.reader.connectionDriver = oracle.jdbc.driver.OracleDriver
<agent_name>.sources.<source_name>.reader.connectionUrl = jdbc:oracle:oci:@
<agent_name>.sources.<source_name>.reader.username = sys as sysdba
<agent_name>.sources.<source_name>.reader.password = sys
<agent_name>.sources.<source_name>.reader.password.cmd = NULL
<agent_name>.sources.<source_name>.reader.table = NULL
<agent_name>.sources.<source_name>.reader.table.columnToCommit = NULL
<agent_name>.sources.<source_name>.reader.table.columnToCommit.type = [TIMESTAMP (default)|NUMERIC|STRING]
<agent_name>.sources.<source_name>.reader.query = NULL
<agent_name>.sources.<source_name>.reader.query.path = NULL
<agent_name>.sources.<source_name>.reader.scaleAwareNumeric = false
<agent_name>.sources.<source_name>.reader.expandBigFloats = false
<agent_name>.sources.<source_name>.duplicatedEventsProcessor = true
<agent_name>.sources.<source_name>.duplicatedEventsProcessor.size = 1000
<agent_name>.sources.<source_name>.duplicatedEventsProcessor.header = true
<agent_name>.sources.<source_name>.duplicatedEventsProcessor.body = true
<agent_name>.sources.<source_name>.duplicatedEventsProcessor.path = last_events.hash_list
```

Default values are written, parameters with NULL have no default value. Most configuration parameters do not require any further explanation. However, some of them are explained below.

You can configure the password by specifying a command with ".reader.password.cmd" parameter. If this parameter is configured, ".reader.password" is ignored. Password is extracted as the first line of the output that the command produces (EOL is removed).

".table", ".query" or ".query.path" parameter must be configured. If ".columnToCommit" is not configured, same query will be always run and comittingFile and committtedValue parameters will be ignored.

If ".columnToCommit" is configured, the value of this column will be committed to "committingFile". This column must be returned by the query. You would need to specify the type of this column in order to build the query properly (default type TIMESTAMP). 

When starting, last committed value is loaded from ".committingFile" if specified. File is created if it does not exist. In case ".committingFile" does not exist or is empty, last committed value will be ".committedValue" if specified.

In case the query is not built properly or you want to use a custom one, you can use ".query" parameter (or ".query.path" for loading the query from a file). In that case ".table" and "columnToCommit.type" parameters are ignored. You should use the following syntax:

```
SELECT * FROM table_name [WHERE column_name >= ':committed_value'] ORDER BY column_name
```

NOTICE: default generated query makes use of ">=" for filtering new rows. Duplicated rows will be loaded from source table but they will be dropped by "duplicatedEventsProcessor" (default enabled). If "duplicatedEventsProcessor" is disabled and default generated query is used, duplicated events will be produced. You can configure your custom query in order to avoid duplicates when "duplicatedEventsProcessor" is not enabled. For more details of default behaviour please refer to: https://its.cern.ch/jira/browse/HRFAL-13

Some tips:
* ORDER BY clause is strongly recommended to be used since last value from "column to commit" will be used in further queries to get only last rows.
* If no value has been committed yet, part of the query between [] is removed. This is the case when we first start Flume and  ".committtedValue" is not configured.
* All occurrences of :committed_value will be replaced by last committed value.
* :committed_value should be between [], but if there will be always a committed value you do not need to place them.

Custom query example:

```
reader.query = SELECT * FROM UNIFIED_AUDIT_TRAIL [WHERE EVENT_TIMESTAMP > TIMESTAMP ':committed_value'] ORDER BY EVENT_TIMESTAMP
```

Query to be executed when a value has not been committed yet:

```
SELECT * FROM UNIFIED_AUDIT_TRAIL ORDER BY EVENT_TIMESTAMP
```

As soon as a value has been committed, query will be like:

```
SELECT * FROM UNIFIED_AUDIT_TRAIL WHERE EVENT_TIMESTAMP > TIMESTAMP '2013-11-08 12:11:31.123123 Europe/Zurich' ORDER BY EVENT_TIMESTAMP
```

The configuration parameters ".scaleAwareNumeric" and ".expandBigFloats" have influence on the output of reader.

If ".scaleAwareNumeric" is enabled, the reader checks the scale of NUMERIC/NUMBER types, and if it is 0, it will treat the type as an integer value. This means that a number like `1234` will show up as `1234` instead of `1234.0` in the output.

The parameter ".expandBigFloats" changes the output of floating-point types when enabled. A number as `2245245222.222` will be displayed as `2.245245222222E9` in a JSON event by default. When this parameter is enabled, the output will change to `2245245222.222`.

## duplicatedEventsProcessor

It compares new Events with last events. A set of last Event hashes is maintained, the size of this set can be configured with "size" parameter, default size is 1000.

Event's hash is calculated by default from headers (disabled if "header" parameter is set to false) and body (disabled if "body" parameter is set to false).

List of hashes is persisted into disk, it allows to maintain the list in case agent is restarted. File to persist hashes can be configured by "path" parameter.