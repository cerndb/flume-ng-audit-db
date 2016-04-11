package ch.cern.db.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;


public class SUtilsTest extends Assert {
	
	@Test
	public void toLines(){
		String input = "line 1\n"
					 + " line 2\n"
					 + "another line \n"
					 + " \n"
					 + "\n";
		
		List<String> expectedLines = new LinkedList<String>();
		expectedLines.add("line 1");
		expectedLines.add(" line 2");
		expectedLines.add("another line ");
		expectedLines.add(" ");
		
		assertEquals(expectedLines, SUtils.toLines(input));
	}
	
	@Test
	public void grep(){
		List<String> lines = new LinkedList<String>();
		lines.add("line one");
		lines.add(" line 2");
		lines.add("another liiine ");
		lines.add(" ");
		lines.add("");
			
		List<String> expectedLines = new LinkedList<String>();
		expectedLines.add("line one");
		expectedLines.add(" line 2");
		
		assertEquals(expectedLines, SUtils.grep(lines, "line"));
		
		expectedLines.clear();
		expectedLines.add(" line 2");
		
		assertEquals(expectedLines, SUtils.grep(lines, Pattern.compile(".*\\d$")));
	}

	@Test
	public void linesFromTo(){
		String input = "line one\n"
				 + " line 2\n"
				 + "line \n"
				 + "another liiine \n"
				 + " \n"
				 + "\n"
				 + "another liiine \n";
		List<String> lines = SUtils.toLines(input);
		
		List<String> expectedLines = new LinkedList<String>();
		expectedLines.add(" line 2");
		expectedLines.add("line ");
		expectedLines.add("another liiine ");
		expectedLines.add(" ");
		
		assertEquals(expectedLines, SUtils.linesFromTo(lines, 
				Pattern.compile(".*line \\d.*"), 
				SUtils.EMPTY_LINE_PATTERN));
		
		expectedLines.clear();
		expectedLines.add("line one");
		expectedLines.add(" line 2");
		
		assertEquals(expectedLines, SUtils.linesFromTo(lines, 
				Pattern.compile(".*line.*"), 
				Pattern.compile(".*line.*")));
		
	}
	
	@Test
	public void join(){
		List<String> inputLines = new LinkedList<String>();
		inputLines.add("line 1");
		inputLines.add(" line 2");
		inputLines.add("another line ");
		inputLines.add(" ");
		
		String expected = "line 1\n"
				 + " line 2\n"
				 + "another line \n"
				 + " \n";
		
		assertEquals(expected, SUtils.join(inputLines, '\n'));
	}
	
	@Test
	public void linesFrom(){
		List<String> inputLines = new LinkedList<String>();
		inputLines.add("line 1");
		inputLines.add(" line 2");
		inputLines.add("another line ");
		inputLines.add(" ");
		
		List<String> expectedLines = new LinkedList<String>();
		expectedLines.add(" line 2");
		expectedLines.add("another line ");
		expectedLines.add(" ");
		
		assertEquals(expectedLines, SUtils.linesFrom(inputLines, Pattern.compile("^\\s.*")));
	}
	
	@Test
	public void linesBefore(){
		List<String> inputLines = new LinkedList<String>();
		inputLines.add("line 1");
		inputLines.add(" line 2");
		inputLines.add("another line ");
		inputLines.add(" ");
		
		List<String> expectedLines = new LinkedList<String>();
		expectedLines.add(" line 2");
		
		assertEquals(expectedLines, SUtils.linesBefore(inputLines, Pattern.compile(".*another.*"), 1));
		
		expectedLines.clear();
		expectedLines.add("line 1");
		expectedLines.add(" line 2");
		
		assertEquals(expectedLines, SUtils.linesBefore(inputLines, Pattern.compile(".*another.*"), 4));
	}
	
}
