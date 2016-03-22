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

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Assert;
import org.junit.Test;

import ch.cern.db.flume.sink.elasticsearch.serializer.JSONtoElasticSearchEventSerializer;


public class JSONtoElasticSearchEventSerializerTest{

	@Test
	public void serialize() throws IOException{
		String json = "{\"menu\":{\"id\":\"file\",\"value\":\"File\",\"popup\""
				+ ":{\"menuitem\":[{\"value\":\"New\",\"onclick\":\"CreateNewDoc()\"}]}}}";
		Event event = EventBuilder.withBody(json.getBytes());
		
		JSONtoElasticSearchEventSerializer serializer = new JSONtoElasticSearchEventSerializer();
		
		XContentBuilder content = serializer.getContentBuilder(event);
		Assert.assertEquals(json, content.string());
	}
	
}
