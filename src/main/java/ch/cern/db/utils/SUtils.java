/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.utils;

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
		List<String> returnLines = new LinkedList<>();
		
		for (String line : lines)
			if(line.matches(regex))
				returnLines.add(line);
		
		return returnLines;
	}

	public static List<String>  grep(List<String> lines, Pattern pattern) {
		List<String> returnLines = new LinkedList<>();
		
		for (String line : lines)
			if(pattern.matcher(line).matches())
				returnLines.add(line);
		
		return returnLines;
	}

	
}
