package ch.cern.db.audit.flume;

public class Field{
	
	public enum Type {
	    STRING,
	    NUMBER,
	    BOOLEAN
	}
	
	public String name, value;
	
	public Field(String name, String value) {
		this.name = name;
		this.value = value;
	}

	@Override
	public String toString() {
		return "Field [name=" + name + ", value=" + value + "]";
	}
	
}
