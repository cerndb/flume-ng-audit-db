package ch.cern.db.flume.source.deserializer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.db.utils.Pair;
import ch.cern.db.utils.SUtils;

public class RecoveryManagerReport {
	
	private static final Logger logger = LoggerFactory.getLogger(RecoveryManagerReport.class);

	private List<String> lines;
	
	private static final DateFormat dateFormatter = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
	private static final DateFormat dateFormatterWhenFailing = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");

	private static final Pattern ORAPattern = Pattern.compile("^[ ]?ORA-\\d{5}[:][ ].*");
	private static final Pattern RMANPattern = Pattern.compile("^[ ]?RMAN-\\d{5}[:][ ].*");
	private static final Pattern returnCodePattern = Pattern.compile(".*return code: \\<(.*)\\>.*");
	private static final Pattern finishTimeWhenFailingPattern = 
			Pattern.compile("([A-z]{3} [A-z]{3} \\d\\d \\d\\d:\\d\\d:\\d\\d [A-z]{3} \\d\\d\\d\\d)  : RunTime\\.RunStr failed.*");
	private static final Pattern startingTimePattern = 
			Pattern.compile(".*Starting backup at \\d\\d-[A-Z]{3}-\\d\\d\\d\\d \\d\\d:\\d\\d:\\d\\d.*");
	private static final Pattern finishedTimePattern = 
			Pattern.compile(".*Finished backup at \\d\\d-[A-Z]{3}-\\d\\d\\d\\d \\d\\d:\\d\\d:\\d\\d.*");
	
	public RecoveryManagerReport(String recoveryManagerOutput) {
		lines = SUtils.toLines(recoveryManagerOutput);
	}

	public List<Pair<Integer, String>> getRMANs() {
		List<Pair<Integer, String>> list = new LinkedList<>();
		
		for (String line : SUtils.grep(lines, RMANPattern)) {
			String[] splitted = line.trim().split(":", 2);
				
			list.add(new Pair<Integer, String>(Integer.valueOf(splitted[0].split("-")[1]), splitted[1]));
		}
		
		return list;
	}
	
	public List<Pair<Integer, String>> getORAs() {
		List<Pair<Integer, String>> list = new LinkedList<>();
		
		for (String line : SUtils.grep(lines, ORAPattern)) {
			String[] splitted = line.trim().split(":", 2);
				
			list.add(new Pair<Integer, String>(Integer.valueOf(splitted[0].split("-")[1]), splitted[1]));
		}
		
		return list;
	}

	public Date getStartingTime() {		
		List<String> grep = SUtils.grep(lines, startingTimePattern);
		if(grep.size() < 1)
			return null;
		
		String line = grep.get(0);
		
		line = line.replace("Starting backup at ", "").trim();
		
		try {
			return dateFormatter.parse(line);
		} catch (ParseException e) {
			logger.error(e.getMessage(), e);
			
			return null;
		}
	}
	
	public Date getFinishTime() {		
		List<String> grep = SUtils.grep(lines, finishedTimePattern);
		if(grep.size() < 1)
			return getFinishTimeWhenFailed();
		
		String line = grep.get(0);
		
		line = line.replace("Finished backup at ", "").trim();
		
		try {
			return dateFormatter.parse(line);
		} catch (ParseException e) {
			logger.error(e.getMessage(), e);
			
			return null;
		}
	}

	private Date getFinishTimeWhenFailed() {
		String line = lines.get(1);
		
		Matcher m = finishTimeWhenFailingPattern.matcher(line.trim());
		
		if(m.find())
			try {
				return dateFormatterWhenFailing.parse(m.group(1));
			} catch (ParseException e) {
				logger.error(e.getMessage(), e);
				
				return null;
			}
		
		return null;
	}

	public Integer getReturnCode() {
		String line = lines.get(1);
		
		Matcher m = returnCodePattern.matcher(line);
		
		if(m.find())
			return Integer.parseInt(m.group(1));
		
		return null;
	}
	
}
