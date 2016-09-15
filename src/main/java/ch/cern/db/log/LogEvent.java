package ch.cern.db.log;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import ch.cern.db.utils.SUtils;

public class LogEvent {
	
	private Date timestmap;
	
	private String text;

	public LogEvent(Date timestamp, String text) {
		this.timestmap = timestamp;
		this.text = text;
	}

	public LogEvent(DateFormat dateFormat, String text) throws ParseException {
		String first_line = SUtils.getFirstLine(text);
		this.timestmap = extractTimestamp(dateFormat, first_line);
		
		this.text = text;		
	}

	public String getText() {
		return text;
	}

	public Date getTimestamp(){
		return timestmap;
	}

	public static Date extractTimestamp(DateFormat dateFormat, String line) throws ParseException {
		return dateFormat.parse(line);
	}

	public static boolean isNewEvent(DateFormat dateFormat, String line) {
		try {
			return extractTimestamp(dateFormat, line) != null;
		} catch (ParseException e) {
			return false;
		}
	}

	@Override
	public String toString() {
		return new SimpleDateFormat().format(timestmap) + " - " + text;
	}
	
}
