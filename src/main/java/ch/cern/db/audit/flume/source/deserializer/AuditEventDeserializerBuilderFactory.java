package ch.cern.db.audit.flume.source.deserializer;

import java.util.Locale;

import ch.cern.db.audit.flume.source.deserializer.AuditEventDeserializer.Builder;

/**
 * Factory used to register instances of AuditEventDeserializer & their
 * builders, as well as to instantiate the builders.
 */
public class AuditEventDeserializerBuilderFactory {

	public enum AuditEventDeserializerType {
		TEXT(ch.cern.db.audit.flume.source.deserializer.TextAuditEventDeserializer.Builder.class),
		JSON(ch.cern.db.audit.flume.source.deserializer.JSONAuditEventDeserializer.Builder.class);

		private final Class<? extends Builder> builderClass;

		private AuditEventDeserializerType(Class<? extends Builder> builderClass) {
			this.builderClass = builderClass;
		}

		public Class<? extends Builder> getBuilderClass() {
			return builderClass;
		}

	}

	private static Class<? extends Builder> lookup(String name) {
		try {
			return AuditEventDeserializerType.valueOf(name.toUpperCase(Locale.ENGLISH)).getBuilderClass();
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	/**
	 * Instantiate specified class, either alias or fully-qualified class name.
	 */
	@SuppressWarnings("unchecked")
	public static Builder newInstance(String name)
			throws ClassNotFoundException, InstantiationException,
			IllegalAccessException {

		Class<? extends Builder> clazz = lookup(name);
		if (clazz == null) {
			clazz = (Class<? extends Builder>) Class.forName(name);
		}
		return clazz.newInstance();
	}

}
