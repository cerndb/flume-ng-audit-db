# LogFileSource

## How does it work?

In most of the cases, log events contains at the beginning of the line a timestamp, the logic of this source is based on this timestamp. Therefore, log files which does not include a timestamp cannot be processed by this source.

Every line that starts with a timestamp is considered a log event. Lines without timestamp that follow a line with timestamp are included into the event (that behaviour can be disabled). 

By using the log event timestamp, this source keep tracking of what have been already consumed and avoid to generate duplicates. Two events can have the same timestamp, so a duplicated events processor have been put in place in order to avoid duplicates for that reason.

A file which contains the last processed event timestamp is created and updated constantly. In case agent is restarted, it will continue at the same place by loading value from committing file.

This source is compatible with log files which are rolled out. It will check if the file has been rolled out, in such case it will start reading again from the beginning of the file.    

## Configuration

In order to use LogFileSource as source in your Flume agent, you need to specify the type of agent source as:

```
<agent_name>.sources.<source_name>.type = ch.cern.db.flume.source.LogFileSource 
```

Find below all available configuration parameters:

```
<agent_name>.sources.<source_name>.batch.size = 100
<agent_name>.sources.<source_name>.batch.minimumTime = 10000
<agent_name>.sources.<source_name>.reader.path = 
<agent_name>.sources.<source_name>.reader.dateFormat = yyyy-MM-dd'T'HH:mm:ssZ
<agent_name>.sources.<source_name>.reader.severalLines = true
<agent_name>.sources.<source_name>.reader.parser = ch.cern.db.flume.source.reader.log.DefaultLogEventParser$Builder
<agent_name>.sources.<source_name>.reader.committingFile = committed_value.backup
<agent_name>.sources.<source_name>.reader.committtedDate = NULL
<agent_name>.sources.<source_name>.duplicatedEventsProcessor = true
<agent_name>.sources.<source_name>.duplicatedEventsProcessor.size = 1000
<agent_name>.sources.<source_name>.duplicatedEventsProcessor.header = true
<agent_name>.sources.<source_name>.duplicatedEventsProcessor.body = true
<agent_name>.sources.<source_name>.duplicatedEventsProcessor.path = last_events.hash_list
```

Default values are written, parameters with NULL have no default value. Most configuration parameters do not require any further explanation. However, some of them are explained below.

Path to the log file must be always specified with .reader.path parameter.

Format of the timestamp contained in the log files can be specified with .reader.dateFormat parameter. It needs to follow the following rules: http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html

Log files may contain events which are built from several lines. By default, all lines that no contain timestamp will be included into the log event. This can be disabled with .reader.severalLines  parameter, then events will only contain the line with the timestamp and the following lines will be skipped.

When starting, last committed value is loaded from ".committingFile" if specified. File is created if it does not exist. In case ".committingFile" does not exist or is empty, last committed value will be ".committedDate" if specified. NOTE: .committedDate must be always configured following this format "yyyy-MM-dd'T'HH:mm:ssZ" (independently of configured .reader.dateFormat).

## Log event parser

A parser is used to convert log events to Flume events can be implemented and configured with .reader.parser. The default parser creates a Flume event with a header (log_event_timestamp) containing the log event timestamp and the body is the event text. The value of this header is a string with the following format: "yyyy-MM-dd'T'HH:mm:ssZ".

Another parser is included (ToJSONLogEventParser), this parser creates a Flume event with no headers and the body is a JSON object with the timestamp and text of the log event. In order to use this parser, the agent should be configured as following:

```
<agent_name>.sources.<source_name>.reader.parser = ch.cern.db.flume.source.reader.log.ToJSONLogEventParser$Builder
```

A custom parser can be implemented by implementing ch.cern.db.flume.source.reader.log.EventParser interface. The implementation needs to be included in the class path and the FQN of the class configured at .reader.parser parameter.

## duplicatedEventsProcessor

It compares new events with last events. A set of last event hashes is maintained, the size of this set can be configured with "size" parameter, default size is 1000.

Event's hash is calculated by default from headers (disabled if "header" parameter is set to false) and body (disabled if "body" parameter is set to false).

List of hashes is persisted into disk, it allows to maintain the list in case agent is restarted. File to persist hashes can be configured by "path" parameter.