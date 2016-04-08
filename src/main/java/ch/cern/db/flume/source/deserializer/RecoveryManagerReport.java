package ch.cern.db.flume.source.deserializer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.db.utils.Pair;
import ch.cern.db.utils.SUtils;

public class RecoveryManagerReport {
	
	private static final Logger logger = LoggerFactory.getLogger(RecoveryManagerReport.class);

	private List<String> lines;
	
	private static final DateFormat dateFormatter = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

	private static final Pattern ORAPattern = Pattern.compile("^[ ]?ORA-\\d{5}[:][ ].*");
	private static final Pattern RMANPattern = Pattern.compile("^[ ]?RMAN-\\d{5}[:][ ].*");
	private static final Pattern startingTimePattern = 
			Pattern.compile(".*Starting backup at \\d\\d-[A-Z]{3}-\\d\\d\\d\\d \\d\\d:\\d\\d:\\d\\d.*");
	
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
		String startingTimeLine = SUtils.grep(lines, startingTimePattern).get(0);
		
		startingTimeLine = startingTimeLine.replace("Starting backup at ", "").trim();
		
		try {
			return dateFormatter.parse(startingTimeLine);
		} catch (ParseException e) {
			logger.error(e.getMessage(), e);
			
			return null;
		}
	}
	
}
