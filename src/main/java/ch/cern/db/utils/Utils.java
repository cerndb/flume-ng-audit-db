package ch.cern.db.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Utils {

	public static String runCommand(String command) throws IOException, InterruptedException{
		Process p = Runtime.getRuntime().exec(command);
	    p.waitFor();

	    BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

	    String line = "";			
	    StringBuilder sb = new StringBuilder();
		while ((line = reader.readLine())!= null)
	    	sb.append(line + "\n");

	    return sb.toString();
	}
	
}
