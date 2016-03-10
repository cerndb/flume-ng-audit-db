package ch.cern.db.flume;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JSONEvent implements Event{
	
	private Map<String, String> headers;
	private JsonObject json;
	
	public JSONEvent() {
		headers = new HashMap<String, String>();
		json = new JsonObject();
	}

	@Override
	public Map<String, String> getHeaders() {
		return headers;
	}

	@Override
	public void setHeaders(Map<String, String> headers) {
		this.headers = headers;
	}
	
	public void addProperty(String name, Object value){
		if(value instanceof Number){
			json.addProperty(name, (Number) value);
		}else if(value instanceof Boolean){
			json.addProperty(name, (Boolean) value);
		}else if(value instanceof JsonElement){
			json.add(name, (JsonElement) value);
		}else{
			json.addProperty(name, (String) value);
		}
	}
	
	public JsonObject getJsonObject(){
		return json;
	}

	@Override
	public byte[] getBody() {
		return json.toString().getBytes();
	}

	@Override
	public void setBody(byte[] body) {
		json = new JsonParser().parse(new String(body)).getAsJsonObject();
	}

	@Override
	public String toString() {
		return "JSONEvent [headers=" + headers + ", body=" + json + "]";
	}
	
}
