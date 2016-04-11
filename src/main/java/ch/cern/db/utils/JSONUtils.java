package ch.cern.db.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class JSONUtils {

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:SSZ");
	
	public static String to(Date date){
		if(date == null)
			return null;
		
		return dateFormat.format(date);
	}
	
}
