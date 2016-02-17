package ch.cern.db.audit.flume.source.reader;

import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.EventReader;
import org.apache.flume.conf.Configurable;

import java.io.IOException;

/**
 * A reliable event reader.
 * Clients must call commit() after each read operation, otherwise the
 * implementation must reset its internal buffers and return the same events
 * as it did previously.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ReliableEventReader extends EventReader {

	/**
	 * Indicate to the implementation that the previously-returned events have
	 * been successfully processed and committed.
	 * 
	 * @throws IOException
	 */
	public void commit() throws IOException;

	/** Builder implementations MUST have a no-arg constructor */
	public interface Builder extends Configurable {
		public ReliableEventReader build();
	}
}
