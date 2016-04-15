package ch.cern.db.flume.source.deserializer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import ch.cern.db.utils.Pair;
import ch.cern.db.utils.SUtils;

public class RecoveryManagerLogFile {
	
	public enum BackDestination{
		DISK,
//		level_1D_newdisk_noarch
//		level_arch_newdisk
		
		TAPE,
//		level_EXEC_BACKUPSET_A
//		level_EXEC_BACKUPSET_F
		
		SNAPSHOT
//		level_EXEC_SNAP
	}

	private static final Logger logger = LoggerFactory.getLogger(RecoveryManagerLogFile.class);
	
	private List<String> lines;

	private int maxLineLength;
	
	private static final DateFormat dateFormatter = new SimpleDateFormat("'['EEE MMM dd HH:mm:ss z yyyy']'");
	
	private static final Pattern getJsonPattern = Pattern.compile("(?s)[^\\{]*(\\{.*\\})[^\\}]*");
	private static final Pattern resourceManagerStartsPattern = Pattern.compile(".*Recovery Manager: Release.*");
	private static final Pattern resourceManagerEndsPattern = Pattern.compile(".*Recovery Manager complete.*");
	
	public RecoveryManagerLogFile(ResettableInputStream in, int maxLineLength) throws IOException {
		this.maxLineLength = maxLineLength;
		
		lines = readAllLines(in);
	}

	private List<String> readAllLines(ResettableInputStream in) throws IOException {
		List<String> lines = new LinkedList<>();
		
		String line = readLine(in);
		while(line != null){
			lines.add(line);
			line = readLine(in);
		}
		
		return lines;
	}

	private String readLine(ResettableInputStream in) throws IOException {
		StringBuilder sb = new StringBuilder();
		int c;
		int readChars = 0;
		while ((c = in.readChar()) != -1) {
			readChars++;

			// FIXME: support \r\n
			if (c == '\n') {
				break;
			}

			sb.append((char) c);

			if (readChars >= maxLineLength) {
				logger.warn("Line length exceeds max ({}), truncating line!", maxLineLength);
				break;
			}
		}

		if (readChars > 0) {
			return sb.toString();
		} else {
			return null;
		}
	}

	private String[] getFieldsFirstLine() {
		List<String> grep = SUtils.grep(lines, "^\\[.*");
		
		if(grep.size() == 0)
			return new String[0];
		
		return grep.get(0).split("\\s+(?![^\\[]*\\])");
	}
	
	public Object getStartTimestamp() {
		String[] fieldsFirstLine = getFieldsFirstLine();
		
		try {
			return (Date) dateFormatter.parse(fieldsFirstLine[0]);
		} catch (Exception e) {
			logger.trace("Error when parsing: " + e.getMessage());
		}
		
		return null;
	}

	public String getBackupType() {
		try{
			return getFieldsFirstLine()[1];
		} catch (Exception e) {
			logger.trace("Error when parsing: " + e.getMessage());
		}
		
		return null;
	}

	public Object getEntityName() {
		try{
			return getFieldsFirstLine()[2];
		} catch (Exception e) {
			logger.trace("Error when parsing: " + e.getMessage());
		}
		
		return null;
	}

	public List<Pair<String, String>> getProperties() {
		List<Pair<String, String>> properties = new LinkedList<>();

		for (String line : lines) {
			Matcher m = SUtils.PROPERTY_PATTERN.matcher(line);
			
			if(m.find())
				properties.add(new Pair<String, String>(m.group(1), m.group(2)));
		}
		
		return properties;
	}

	public String getJSONString(String regex) {
		List<String> regexLines = SUtils.linesFromTo(lines, 
				Pattern.compile(".*" + regex + ".*"), 
				SUtils.EMPTY_LINE_PATTERN);
		
		Matcher matcher = getJsonPattern.matcher(SUtils.join(regexLines, '\n'));
		
		if(matcher.matches())
			return matcher.group(1);
		
		return null;
	}
	
	public String getVParams() {
		return getJSONString("Main: params passed: \\$v_params");
	}

	public JsonArray getMountPointNASRegexResult() {
		String jsonString = getJSONString("RunTime\\.GetMountPointNASRegex : result: \\$VAR1");
		
		JsonArray newArray = new JsonArray();
		
		if(jsonString == null)
			return newArray;
		
		JsonParser parser = new JsonParser();
		
		JsonObject o;
		try{
			o = parser.parse(jsonString).getAsJsonObject();
		}catch(Exception e){
			return newArray;
		}
		
		for (Entry<String, JsonElement> element : o.entrySet()) {
			JsonObject newObject = new JsonObject();
			
			try{
				newObject.addProperty("controllerlif", element.getKey());
			}catch(Exception e){}
			try{
				newObject.addProperty("mountondbserver", ((JsonArray) element.getValue()).get(0).getAsString());
			}catch(Exception e){}
			
			newArray.add(newObject);
		}
		
		return newArray;
	}

	public JsonArray getVolInfoBackuptoDiskFinalResult() {
		String jsonString = getJSONString("RunTime\\.GetVolInfoBackuptoDisk : final result \\$VAR1");
		
		JsonArray newArray = new JsonArray();
		
		if(jsonString == null)
			return newArray;
		
		JsonParser parser = new JsonParser();
		
		JsonObject o;
		try{
			o = parser.parse(jsonString).getAsJsonObject();
		}catch(Exception e){
			return newArray;
		}
		
		for (Entry<String, JsonElement> element : o.entrySet()) {
			try{
				newArray.add((JsonObject) element.getValue());
			}catch(Exception e){}
		}
		
		return newArray;
	}

	public JsonArray getValuesOfFilesystems() {
		String jsonString = getJSONString("values of filesystems \\$filesystems");
		
		JsonArray newArray = new JsonArray();
		
		if(jsonString == null)
			return newArray;
		
		JsonParser parser = new JsonParser();
		
		JsonObject o;
		try{
			o = parser.parse(jsonString).getAsJsonObject();
		}catch(Exception e){
			return newArray;
		}
		
		for (Entry<String, JsonElement> element : o.entrySet()) {
			try{
				newArray.add((JsonObject) element.getValue());
			}catch(Exception e){}
		}
		
		return newArray;
	}

	public String getCreateFilesBackupset() {
		return getJSONString("CreateFiles: begin: array of backupset \\$VAR1");
	}
	
	public List<String> getRecoveryManagerOutputs(){
		List<String> recoveryManagerOutputs = new LinkedList<String>();
		
		@SuppressWarnings("unchecked")
		List<String> linesClone = (LinkedList<String>) ((LinkedList<String>) lines).clone();
		
		List<String> regexLines = SUtils.linesFromTo(linesClone, 
				resourceManagerStartsPattern, 
				resourceManagerEndsPattern);
		
		while(regexLines.size() > 0){
			List<String> prevLines = SUtils.linesBefore(linesClone, resourceManagerStartsPattern, 3);
			prevLines.addAll(regexLines);
			
			recoveryManagerOutputs.add(SUtils.join(prevLines, '\n'));
			
			linesClone = SUtils.linesFrom(linesClone, resourceManagerEndsPattern);
			if(linesClone.size() > 0)
				linesClone.remove(0);
			
			regexLines = SUtils.linesFromTo(linesClone, 
					resourceManagerStartsPattern, 
					resourceManagerEndsPattern);
		}
		
		return recoveryManagerOutputs;
	}

	public List<RecoveryManagerReport> getRecoveryManagerReports() {
		List<String> recoveryManagerOutputs = getRecoveryManagerOutputs();
	
		List<RecoveryManagerReport> reports = new LinkedList<>();
		
		for (String recoveryManagerOutput : recoveryManagerOutputs)
			reports.add(new RecoveryManagerReport(recoveryManagerOutput));
		
		return reports;
	}

	public BackDestination getBackupDestination() {
		String backupType = getBackupType(); 

		if(backupType == null)
			return null;
		
		if(backupType.contains("newdisk"))
			return BackDestination.DISK;
		
		if(backupType.contains("BACKUPSET"))
			return BackDestination.TAPE;
		
		if(backupType.contains("SNAP"))
			return BackDestination.SNAPSHOT;
		
		return null;
	}
}
