/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.source.deserializer;

import org.junit.Assert;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import ch.cern.db.utils.Pair;

public class RecoveryManagerReportTest extends Assert {

	@Test
	public void successfulReport() throws IOException{
		String input = readFile("src/test/resources/rman-reports/successful.out");
		RecoveryManagerReport report = new RecoveryManagerReport(input);
		
		assertEquals("Tue Apr 05 05:31:54 CEST 2016", report.getStartingTime().toString());
		assertEquals(0, report.getORAs().size());
		assertEquals(0, report.getRMANs().size());
		assertNull(report.getReturnCode());
		assertEquals("Tue Apr 05 05:32:16 CEST 2016", report.getFinishTime().toString());
		
	}

	@Test
	public void failedReport() throws IOException{
		String input = readFile("src/test/resources/rman-reports/failed.out");
		RecoveryManagerReport report = new RecoveryManagerReport(input);
		
		assertEquals("Wed Jan 27 18:45:27 CET 2016", report.getStartingTime().toString());
		
		List<Pair<Integer, String>> oras = report.getORAs();
		assertEquals(3, oras.size());
		assertEquals(27028, oras.get(0).getFirst(), 0);
		assertEquals("skgfqcre: sbtbackup returned error", oras.get(0).getSecond());
		assertEquals(19511, oras.get(1).getFirst(), 0);
		assertEquals("Error received from media manager layer, error text:", oras.get(1).getSecond());
		assertEquals(19600, oras.get(2).getFirst(), 0);
		assertEquals("input file is backup piece  (/backup/dbs01/EDHP/EDHP_20160127_scqsd839_1_1_arch)", oras.get(2).getSecond());
		
		List<Pair<Integer, String>> rmans = report.getRMANs();
		assertEquals(2, rmans.size());
		assertEquals(3009, rmans.get(0).getFirst(), 0);
		assertEquals("failure of backup command on ORA_SBT_TAPE_1 channel at 01/27/2016 18:51:08", rmans.get(0).getSecond());
		assertEquals(569, rmans.get(1).getFirst(), 0);
		assertEquals("=============== ERROR MESSAGE STACK FOLLOWS ===============", rmans.get(1).getSecond());
		
		assertEquals(256, report.getReturnCode(), 0);
		assertEquals("Wed Jan 27 18:51:10 CET 2016", report.getFinishTime().toString());
	}
	
	private String readFile(String input_file) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(input_file));
		
		try {
		    StringBuilder sb = new StringBuilder();
		    String line = br.readLine();

		    while (line != null) {
		        sb.append(line);
		        sb.append(System.lineSeparator());
		        line = br.readLine();
		    }
		    
		    return sb.toString();
		} finally {
		    br.close();
		}
	}
	
}
