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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

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
		XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(event.getBody());
		parser.close();
		return XContentFactory.jsonBuilder().copyCurrentStructure(parser);
	}

}
