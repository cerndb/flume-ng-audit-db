package ch.cern.flume.source.oracle;

import java.util.LinkedList;
import java.util.List;

public class AuditEvent {
	
	List<Field> fields = null;
	
	public AuditEvent() {
		fields = new LinkedList<Field>();
	}

	public void addField(String name, String value) {
		fields.add(new Field(name, value));
	}
	
	@Override
	public String toString() {
		return "AuditEvent [fields=" + fields + "]";
	}

	class Field{
		String name, value;
		
		public Field(String name, String value) {
			this.name = name;
			this.value = value;
		}

		@Override
		public String toString() {
			return "Field [name=" + name + ", value=" + value + "]";
		}
		
	}
	
}
