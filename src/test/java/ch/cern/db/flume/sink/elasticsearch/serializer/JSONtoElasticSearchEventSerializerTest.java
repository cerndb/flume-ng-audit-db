/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.sink.elasticsearch.serializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.flume.sink.elasticsearch.serializer.JSONtoElasticSearchEventSerializer;


public class JSONtoElasticSearchEventSerializerTest{

	@Test
	public void serialize() throws IOException{
		String json = "{" +
		        "\"data\":\"Click Here\"," +
		        "\"size\":36," +
		        "\"style\":\"bold\"," +
		        "\"name\":\"text1\"," +
		        "\"hOffset\":250.54," +
		        "\"vOffset\":100.13," +
		        "\"nullValue\":null," +
		        "\"alignment\":\"center\"," +
		        "\"onMouseUp\":\"sun1.opacity = (sun1.opacity / 100) * 90;\"" +
		    	"}";
		Event event = EventBuilder.withBody(json.getBytes());
		
		JSONtoElasticSearchEventSerializer serializer = new JSONtoElasticSearchEventSerializer();
		
		XContentBuilder content = serializer.getContentBuilder(event);
		Assert.assertEquals(json, content.string());
	}
	
	@Test
	public void serializeWithHeaders() throws IOException{
		String json = "{" +
		        "\"data\": \"Click Here\"," +
		        "\"size\": 36," +
		        "\"style\": \"bold\"," +
		        "\"name\": \"text1\"," +
		        "\"hOffset\": 250.54," +
		        "\"vOffset\": 100.13," +
		        "\"alignment\": \"center\"," +
		        "\"center\": true," +
		        "\"onMouseUp\": \"sun1.opacity = (sun1.opacity / 100) * 90;\"" +
		    	"}";
		Event event = EventBuilder.withBody(json.getBytes());
		
		Map<String, String> headers = new HashMap<>();
		headers.put("head1", "value1");
		headers.put("head2", "value2");
		event.setHeaders(headers );
		
		JSONtoElasticSearchEventSerializer serializer = new JSONtoElasticSearchEventSerializer();
		
		XContentBuilder content = serializer.getContentBuilder(event);
		System.out.println(content.string());
		Assert.assertEquals("{\"head1\":\"value1\","
				+ "\"head2\":\"value2\","
				+ "\"data\":\"Click Here\","
				+ "\"size\":36,"
				+ "\"style\":\"bold\","
				+ "\"name\":\"text1\","
				+ "\"hOffset\":250.54,"
				+ "\"vOffset\":100.13,"
				+ "\"alignment\":\"center\","
				+ "\"center\":true,"
				+ "\"onMouseUp\":\"sun1.opacity = (sun1.opacity / 100) * 90;\"}", 
				content.string());
	}
	
}
