package ch.cern.flume.source.oracle;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.flume.Event;

public class AvroAuditEventDeserialezer implements AuditEventDeserialezer {

	Schema schema = null;
	
	@Override
	public Event process(AuditEvent event) {
		// TODO Auto-generated method stub
		return null;
	}

	public void setFieldNames(List<String> columnNames) {
		
		List<Field> fields = new LinkedList<Field>();
		
		for (Iterator<String> iterator = columnNames.iterator(); iterator.hasNext();) {
			String name = iterator.next();
			
			fields.add(new Field(name, 
					Schema.create(Type.STRING), 
					null, 
					null));
		}
		
		schema = Schema.createRecord(fields);
	}

}
