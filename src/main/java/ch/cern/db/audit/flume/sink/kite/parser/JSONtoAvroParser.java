package ch.cern.db.audit.flume.sink.kite.parser;

import static org.apache.flume.sink.kite.DatasetSinkConstants.AVRO_SCHEMA_LITERAL_HEADER;
import static org.apache.flume.sink.kite.DatasetSinkConstants.AVRO_SCHEMA_URL_HEADER;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.kite.NonRecoverableEventException;
import org.apache.flume.sink.kite.parser.EntityParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * An {@link EntityParser} that parses Avro serialized bytes from an event.
 * 
 * The Avro schema used to serialize the data should be set as either a URL or
 * literal in the flume.avro.schema.url or flume.avro.schema.literal event
 * headers respectively.
 */
public class JSONtoAvroParser implements EntityParser<GenericRecord> {

	static Configuration conf = new Configuration();

	/**
	 * A cache of literal schemas to avoid re-parsing the schema.
	 */
	private static final LoadingCache<String, Schema> schemasFromLiteral = CacheBuilder
			.newBuilder().build(new CacheLoader<String, Schema>() {
				@Override
				public Schema load(String literal) {
					Preconditions.checkNotNull(literal,
									"Schema literal cannot be null without a Schema URL");
					return new Schema.Parser().parse(literal);
				}
			});

	/**
	 * A cache of schemas retrieved by URL to avoid re-parsing the schema.
	 */
	private static final LoadingCache<String, Schema> schemasFromURL = CacheBuilder
			.newBuilder().build(new CacheLoader<String, Schema>() {
				@Override
				public Schema load(String url) throws IOException {
					Schema.Parser parser = new Schema.Parser();
					InputStream is = null;
					try {
						FileSystem fs = FileSystem.get(URI.create(url), conf);
						if (url.toLowerCase(Locale.ENGLISH)
								.startsWith("hdfs:/")) {
							is = fs.open(new Path(url));
						} else {
							is = new URL(url).openStream();
						}
						return parser.parse(is);
					} finally {
						if (is != null) {
							is.close();
						}
					}
				}
			});

	/**
	 * The schema of the destination dataset.
	 * 
	 * Used as the reader schema during parsing.
	 */
	private final Schema datasetSchema;

	/**
	 * A cache of DatumReaders per schema.
	 */
	private final LoadingCache<Schema, DatumReader<GenericRecord>> readers = CacheBuilder
			.newBuilder().build(
					new CacheLoader<Schema, DatumReader<GenericRecord>>() {
						@Override
						public DatumReader<GenericRecord> load(Schema schema) {
							// must use the target dataset's schema for reading to ensure the
							// records are able to be stored using it
							return new GenericDatumReader<GenericRecord>(
									schema, datasetSchema);
						}
					});

	/**
	 * The JSON decoder to reuse for event parsing.
	 */
	private JsonDecoder decoder = null;

	/**
	 * Create a new AvroParser given the schema of the destination dataset.
	 * 
	 * @param datasetSchema
	 *            The schema of the destination dataset.
	 */
	private JSONtoAvroParser(Schema datasetSchema) {
		this.datasetSchema = datasetSchema;
	}

	/**
	 * Parse the entity from the body in JSON of the given event.
	 * 
	 * @param event
	 *            The event to parse.
	 * @param reuse
	 *            If non-null, this may be reused and returned from this method.
	 * @return The parsed entity as a GenericRecord.
	 * @throws EventDeliveryException
	 *             A recoverable error such as an error downloading the schema
	 *             from the URL has occurred.
	 * @throws NonRecoverableEventException
	 *             A non-recoverable error such as an unparsable schema or
	 *             entity has occurred.
	 */
	@Override
	public GenericRecord parse(Event event, GenericRecord reuse)
			throws EventDeliveryException, NonRecoverableEventException {
		
		JsonObject parser = new JsonParser().parse(new String(event.getBody())).getAsJsonObject();
		
		GenericRecordBuilder recordBuilder = new GenericRecordBuilder(datasetSchema);
		for (Field field:datasetSchema.getFields()) {
			JsonElement value = parser.get(field.name());
			
			recordBuilder.set(field.name(), value != null ? value.getAsString() : null);
		}

		return recordBuilder.build();
	}

	public static class Builder implements EntityParser.Builder<GenericRecord> {

		@Override
		public EntityParser<GenericRecord> build(Schema datasetSchema, Context config) {
			return new JSONtoAvroParser(datasetSchema);
		}

	}
}
