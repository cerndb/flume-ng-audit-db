package ch.cern.db.audit.flume.sink.elasticsearch.serializer;

import java.io.IOException;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Assert;
import org.junit.Test;


public class JSONtoElasticSearchEventSerializerTests{

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
