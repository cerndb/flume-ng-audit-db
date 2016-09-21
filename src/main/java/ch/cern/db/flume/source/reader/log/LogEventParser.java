package ch.cern.db.flume.source.reader.log;

import org.apache.flume.Context;
import org.apache.flume.Event;

import ch.cern.db.log.LogEvent;

public interface LogEventParser {

	public Event parse(LogEvent event);

	public interface Builder {
		public LogEventParser build(Context context);
	}

}
