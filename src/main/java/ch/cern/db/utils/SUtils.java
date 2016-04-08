/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * String utilities
 * @author daniellanzagarcia
 *
 */
public class SUtils {

	public static List<String> grep(List<String> lines, String regex) {
		return grep(lines, Pattern.compile(regex));
	}

	public static List<String>  grep(List<String> lines, Pattern pattern) {
		List<String> returnLines = new LinkedList<>();
		
		for (String line : lines)
			if(pattern.matcher(line).matches())
				returnLines.add(line);
		
		return returnLines;
	}

	public static List<String> linesFromTo(List<String> lines, Pattern patternStart, Pattern patternEnd) {
		List<String> returnLines = new LinkedList<>();
		
		Iterator<String> it = lines.iterator();
		
		while (it.hasNext()){
			String line = it.next();
			
			if(patternStart.matcher(line).matches()){
				returnLines.add(line);
				
				break;
			}
		}
		
		while (it.hasNext()){
			String line = it.next();
			
			returnLines.add(line);
			
			if(patternEnd.matcher(line).matches())
				break;
		}
		
		return returnLines;	
	}

	public static String join(List<String> lines, char delim) {
		StringBuilder sb = new StringBuilder();
		
		for (String string : lines) {
			sb.append(string);
			sb.append(delim);
		}
		
		return sb.toString();
	}

	public static List<String> linesFrom(List<String> linesClone, Pattern pattern) {
		List<String> returnLines = new LinkedList<>();
		
		Iterator<String> it = linesClone.iterator();
		
		while (it.hasNext()){
			String line = it.next();
			
			if(pattern.matcher(line).matches()){
				break;
			}
		}
		
		while (it.hasNext()){
			String line = it.next();
			
			returnLines.add(line);
		}
		
		return returnLines;	
	}

	public static List<String> toLines(String in) {
		return Arrays.asList(in.split("\n"));
	}

	public static List<String> linesBefore(List<String> lines, Pattern pattern, int number) {
		LimitedQueue<String> returnLines = new LimitedQueue<String>(number);
		
		Iterator<String> it = lines.iterator();
		
		while (it.hasNext()){
			String line = it.next();
			
			if(pattern.matcher(line).matches())
				break;
			
			returnLines.add(line);
		}
		
		return returnLines;	
	}
	
}
