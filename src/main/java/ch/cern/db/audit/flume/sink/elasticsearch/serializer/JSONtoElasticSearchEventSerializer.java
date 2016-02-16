package ch.cern.db.audit.flume.sink.elasticsearch.serializer;

import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.frontier45.flume.sink.elasticsearch2.ElasticSearchEventSerializer;

public class JSONtoElasticSearchEventSerializer implements ElasticSearchEventSerializer {

	@Override
	public void configure(Context context) {
	}

	@Override
	public void configure(ComponentConfiguration conf) {
	}

	@Override
	public XContentBuilder getContentBuilder(Event event) throws IOException {
		return XContentFactory.jsonBuilder().value(new String(event.getBody()));
	}

}
