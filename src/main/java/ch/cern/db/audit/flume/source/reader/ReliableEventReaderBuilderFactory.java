package ch.cern.db.audit.flume.source.reader;

import java.util.Locale;

import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.db.audit.flume.source.reader.ReliableEventReader.Builder;


/**
 * Factory used to register instances of AuditEventDeserializer & their
 * builders, as well as to instantiate the builders.
 */
public class ReliableEventReaderBuilderFactory {
	
	private static final Logger LOG = LoggerFactory.getLogger(ReliableEventReaderBuilderFactory.class);

	public enum Types {
		ORACLE(ch.cern.db.audit.flume.source.reader.ReliableOracleAuditEventReader.Builder.class);

		private final Class<? extends Builder> builderClass;

		private Types(Class<? extends Builder> builderClass) {
			this.builderClass = builderClass;
		}

		public Class<? extends Builder> getBuilderClass() {
			return builderClass;
		}

	}

	private static Class<? extends Builder> lookup(String name) {
		try {
			return Types.valueOf(name.toUpperCase(Locale.ENGLISH)).getBuilderClass();
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	/**
	 * Instantiate specified class, either alias or fully-qualified class name.
	 */
	@SuppressWarnings("unchecked")
	public static Builder newInstance(String name){
		try {
			Class<? extends Builder> clazz = lookup(name);
			if (clazz == null) {
				clazz = (Class<? extends Builder>) Class.forName(name);
			}
			
			return clazz.newInstance();
		} catch (ClassNotFoundException e) {
			LOG.error("Builder class not found. Exception follows.", e);
			throw new FlumeException("ReliableEventReader.Builder not found.", e);
		} catch (InstantiationException e) {
			LOG.error("Could not instantiate Builder. Exception follows.", e);
			throw new FlumeException("ReliableEventReader.Builder not constructable.", e);
		} catch (IllegalAccessException e) {
			LOG.error("Unable to access Builder. Exception follows.", e);
			throw new FlumeException("Unable to access ReliableEventReader.Builder.", e);
		}
	}

}
