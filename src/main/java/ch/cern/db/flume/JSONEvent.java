/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */

package ch.cern.db.flume;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JSONEvent implements Event{
	
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:SSZ");
	
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
		if(value == null)
			return;
		
		if(value instanceof Date){
			json.addProperty(name, dateFormat.format((Date) value));
		}else if(value instanceof Number){
			json.addProperty(name, (Number) value);
		}else if(value instanceof Boolean){
			json.addProperty(name, (Boolean) value);
		}else if(value instanceof JsonElement){
			json.add(name, (JsonElement) value);
		}else{
			json.addProperty(name, value.toString());
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
