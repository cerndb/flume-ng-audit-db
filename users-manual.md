# Users manual

Find in this section all the documentation users may require.

## Version control system

A Git repository is used for the project. 

Find it at: https://gitlab.cern.ch/db/cerndb-infra-flume-ng-audit-db/ 

## Developed components

Several components have been developed for adapting Flume, both in the source and sink side.

* JDBCSource: a custom source which is able to collect data from database tables. It makes use of a ReliableJdbcEventReader which uses a JDBC driver for connecting to a database, so many types of databases are compatible. It provides a reliable way to read data from tables in order to avoid data loss and replicated events. This source produces JSONEvents (implements Flume Event interface), this events are deserialized as a JSON string.
* SpoolDirectorySource: customised version of SpoolDirectorySource which only consume closed files. It uses "lsof" command to check if file is open.
* RecoveryManagerDeserializer: deserializer for CERN Recovery Manager logs. It parses log files to JSON events extracting most important information. 
* Interceptors that can modify events produced in the source:
    * DropNoJSONEventsInterceptor: drop all events which are not of the class JSONEvents.
    * JSONEventToCSVInterceptor: convert JSONEvents into normal Flume Events which body is a CSV with the values. Headers are copied. No JSONEvents are not touched.
    * DropDuplicatedEventsInterceptor: drop duplicated events. It only checks with the last "size" events. WARNING: this interceptor will drop events in case transaction to channel fails. In case the agent is restarted, hash for last events is lost so duplicates can appear.
* For some sinks, you may need to implement a custom parser for Flume Events:
    * JSONtoAvroParser: for Kite sink, this parser converts Flume Events which body is JSON into Avro records.
    * JSONtoElasticSearchEventSerializer: for Elasticsearch sink, this parser converts Flume Events which body is JSON into Elasticsearch XContentBuilder.