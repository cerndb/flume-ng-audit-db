package ch.cern.db.flume;

import java.util.LinkedList;
import java.util.List;

public class AuditEvent {
	
	List<Field> fields = null;
	
	public AuditEvent() {
		fields = new LinkedList<Field>();
	}

	public void addField(String name, Object value) {
		fields.add(new Field(name, value));
	}
	
	public List<Field> getFields() {
		return fields;
	}
	
	@Override
	public String toString() {
		return "AuditEvent [fields=" + fields + "]";
	}

	public void addField(Field field) {
		fields.add(field);
	}
	
	public class Field{
		
		public String name;
		
		public Object value;
		
		public Field(String name, Object value) {
			this.name = name;
			this.value = value;
		}

		@Override
		public String toString() {
			return "Field [name=" + name + ", value=" + value + "]";
		}
		
	}
}
