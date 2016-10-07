/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.sink.elasticsearch.serializer;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import ch.cern.db.flume.sink.elasticsearch.ContentBuilderUtil;
import ch.cern.db.flume.sink.elasticsearch.ElasticSearchEventSerializer;

public class JSONtoElasticSearchEventSerializer implements ElasticSearchEventSerializer {

	@Override
	public void configure(Context context) {
	}

	@Override
	public void configure(ComponentConfiguration conf) {
	}

	@Override
	public XContentBuilder getContentBuilder(Event event) throws IOException {
		XContentBuilder builder = jsonBuilder().startObject();
		
		appendHeaders(builder, event);
		appendBody(builder, event);
		
		return builder;
	}

	private void appendHeaders(XContentBuilder builder, Event event) throws IOException {
		Map<String, String> headers = event.getHeaders();
		
		for (String key : headers.keySet()) {
			ContentBuilderUtil.appendField(builder, key, headers.get(key).getBytes(charset));
		}
	}
	
	private void appendBody(XContentBuilder builder, Event event) throws IOException {
		JsonParser parser = new JsonParser();
		
		JsonObject json = parser.parse(new String(event.getBody())).getAsJsonObject();
		
		for (Entry<String, JsonElement> property : json.entrySet()) {
		
			if(property.getValue().isJsonNull()){
				builder.nullField(property.getKey());
				
				continue;
			}
			
			if(!property.getValue().isJsonPrimitive()){
				builder.field(property.getKey(), property.getValue());
				
				continue;
			}
			
			JsonPrimitive primitiveValue = (JsonPrimitive) property.getValue();
			
			if(primitiveValue.isBoolean())
				builder.field(property.getKey(), primitiveValue.getAsBoolean());
			else if(primitiveValue.isNumber())
				if (primitiveValue.getAsString().indexOf('.') != -1)
					builder.field(property.getKey(), primitiveValue.getAsNumber().doubleValue());
				else
					builder.field(property.getKey(), primitiveValue.getAsNumber().longValue());
			else if(primitiveValue.isString())
				builder.field(property.getKey(), primitiveValue.getAsString());
		}
	}

}




