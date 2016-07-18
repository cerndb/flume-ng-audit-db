/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.sink.kite.parser;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.kite.NonRecoverableEventException;
import org.apache.flume.sink.kite.parser.EntityParser;
import org.apache.hadoop.conf.Configuration;

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

	public static final String FIELD_AT_HEADER_PROPERTY = "at_header";

	static Configuration conf = new Configuration();

	/**
	 * The schema of the destination dataset.
	 * 
	 * Used as the reader schema during parsing.
	 */
	private final Schema datasetSchema;

	
	
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
			String at_header = field.getProp(FIELD_AT_HEADER_PROPERTY);
			
			if(at_header != null && at_header.equals(Boolean.TRUE.toString())){
				recordBuilder.set(field.name(), event.getHeaders().get(field.name()));
			}else{
				JsonElement element = parser.get(field.name());
				
				recordBuilder.set(field.name(), getElementAsType(field.schema(), element));
			}
		}

		return recordBuilder.build();
	}
	
	private Object getElementAsType(Schema schema, JsonElement element) {
		if(element == null || element.isJsonNull())
			return null;
		
		switch(schema.getType()){
		case BOOLEAN:
			return element.getAsBoolean();
		case DOUBLE:
			return element.getAsDouble();
		case FLOAT:
			return element.getAsFloat();
		case INT:
			return element.getAsInt();
		case LONG:
			return element.getAsLong();
		case NULL:
			return null;
		case UNION:
			return getElementAsType(schema.getTypes().get(0), element);
//		case FIXED:
//		case ARRAY:
//		case BYTES:
//		case ENUM:
//		case MAP:
//		case RECORD:
//		case STRING:
		default:
			return element.getAsString();
		}
	}

	public static class Builder implements EntityParser.Builder<GenericRecord> {

		@Override
		public EntityParser<GenericRecord> build(Schema datasetSchema, Context config) {
			return new JSONtoAvroParser(datasetSchema);
		}

	}
}
