/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */
package ch.cern.db.flume.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.db.utils.SizeLimitedHashSet;

/**
 * Compare current event with last events to check it was already processed
 */
public class DropDuplicatedEventsProcessor implements Configurable{

	private static final Logger LOG = LoggerFactory.getLogger(DropDuplicatedEventsProcessor.class);

	public static final String PARAM = "duplicatedEventsProcessor";

	public static final String PATH_PARAM = PARAM + ".path";
	public static final String PATH_DEFAULT = "last_events.hash_list";
	private File committing_file = null;

	public static final String SIZE_PARAM = PARAM + ".size";
	public static final Integer SIZE_DEFAULT = 1000;

	public static final String CHECK_HEADER_PARAM = PARAM + ".header";
	public static final Boolean CHECK_HEADER_DEFAULT = true;
	private boolean checkHeaders;

	public static final String CHECK_BODY_PARAM = PARAM + ".body";
	public static final Boolean CHECK_BODY_DEFAULT = true;
	private boolean checkBody;

	public static final String ALGORITHM_PARAM = PARAM + ".algorithm";
	private HashingAlgorithm algorithm = HashingAlgorithm.JAVA_HASHCODE;
	private MessageDigest messageDigest = null;

	private SizeLimitedHashSet<BigInteger> previous_hashes;

	private SizeLimitedHashSet<BigInteger> hashes_current_batch;

	private enum HashingAlgorithm {
		JAVA_HASHCODE, MD5
	}

	public DropDuplicatedEventsProcessor(){
		Integer size = SIZE_DEFAULT;
		previous_hashes = new SizeLimitedHashSet<BigInteger>(SIZE_DEFAULT);
		hashes_current_batch = new SizeLimitedHashSet<BigInteger>(SIZE_DEFAULT);

		this.checkHeaders = CHECK_HEADER_DEFAULT;
		this.checkBody = CHECK_BODY_DEFAULT;
		LOG.info("Initializated with defaults size="+size+", algorithm="+algorithm.name().toLowerCase()+
				", headers="+checkHeaders+", body="+checkBody);
	}

	@Override
	public void configure(Context context){
		Integer size = context.getInteger(SIZE_PARAM, SIZE_DEFAULT);
		if(size != previous_hashes.size()){
			SizeLimitedHashSet<BigInteger> tmp = previous_hashes;
			previous_hashes = new SizeLimitedHashSet<BigInteger>(size);
			previous_hashes.addAll(tmp.getInmutableList());
			tmp.clear();

			tmp = hashes_current_batch;
			hashes_current_batch = new SizeLimitedHashSet<BigInteger>(size);
			hashes_current_batch.addAll(tmp.getInmutableList());
			tmp.clear();
		}

		this.checkHeaders = context.getBoolean(CHECK_HEADER_PARAM, CHECK_HEADER_DEFAULT);
		this.checkBody = context.getBoolean(CHECK_BODY_PARAM, CHECK_BODY_DEFAULT);

		this.committing_file = new File(context.getString(PATH_PARAM, PATH_DEFAULT));
		loadLastHashesFromFile();

		// Decision based on collision rates
		// hashcode is faster but has a much higher collision probability compared to MD5
		if (size <= 1000) {
			this.algorithm = HashingAlgorithm.JAVA_HASHCODE;
		} else {
			this.algorithm = HashingAlgorithm.MD5;
		}
		// Override with the one from config, if provided
		String algorithmName = context.getString(ALGORITHM_PARAM, algorithm.name());
		if ("md5".equals(algorithmName) || HashingAlgorithm.MD5.name().equals(algorithmName)) {
			algorithm = HashingAlgorithm.MD5;
			try {
				messageDigest = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				throw new FlumeException("MD5 hashing not supported! ", e);
			}
		} else {
			algorithm = HashingAlgorithm.JAVA_HASHCODE;
			messageDigest = null;
		}

		LOG.info("Configured with size="+size+", algorithm="+algorithm.name().toLowerCase()+
				", headers="+checkHeaders+", body="+checkBody+", path="+this.committing_file.getPath());
	}

	private void loadLastHashesFromFile() {
		try {
			if(committing_file.exists()){
				previous_hashes.clear();

				BufferedReader br = new BufferedReader(new FileReader(committing_file));
				String line = null;
				while ((line = br.readLine()) != null) {
					BigInteger hash = new BigInteger(line);

					previous_hashes.add(hash);
				}
				br.close();

				LOG.info("Last hashes loaded from file: " + committing_file);
			}else{
				LOG.info("File for storing last hashes does not exist (" +
						committing_file.getAbsolutePath() + "). Therefore hashes list was not modified");
			}
		} catch (IOException e) {
			throw new FlumeException(e);
		}
	}

	public void commit() {
		try {
			FileWriter fw = new FileWriter(committing_file);

			for(BigInteger hash:previous_hashes.getInmutableList())
				fw.write(hash.toString() + System.lineSeparator());
			for(BigInteger hash:hashes_current_batch.getInmutableList())
				fw.write(hash.toString() + System.lineSeparator());

			fw.close();
		} catch (IOException e) {
			throw new FlumeException(e);
		}

		previous_hashes.addAll(hashes_current_batch.getInmutableList());

		hashes_current_batch.clear();
	}

	public void rollback() {
		hashes_current_batch.clear();
	}

	public Event process(Event event) {
		BigInteger event_hash = generateEventHash(event);

		if(previous_hashes.contains(event_hash)
				|| hashes_current_batch.contains(event_hash)){
			LOG.debug("Event dropped: " + event.toString());

			return null;
		}

		hashes_current_batch.add(event_hash);
		return event;
	}

	private BigInteger generateEventHash(Event event) {
		BigInteger headers_hash = BigInteger.ZERO;
		BigInteger body_hash = BigInteger.ZERO;
		if(checkHeaders)
			headers_hash = hashMap(event.getHeaders());


		if(checkBody)
			body_hash = hashString(new String(event.getBody()));

		if(checkHeaders && checkBody)
			return headers_hash.xor(body_hash);
		if (checkHeaders && !checkBody)
			return headers_hash;
		if (!checkHeaders && checkBody)
			return body_hash;
		//else all events will be removed due to hash will be always 0
			return BigInteger.ZERO;
	}

	/**
	 * Creates a hash from a map of string keys and values.
	 * @param map The input map.
	 * @return A hash generated from the input map.
     */
	private BigInteger hashMap(Map<String, String> map) {
		BigInteger hash = BigInteger.ZERO;
		// THe XOR operation ensures that the order of elements does not matter
		for (Map.Entry<String, String> entry : map.entrySet()) {
			// In the original implementation, the hashcodes of the following maps would equal, because the
			// key and value are XOR-d:
			// Map 1: foo -> bar
			// Map 2: bar -> foo
			// I will add a small offset to the value hash to avoid this.
			BigInteger hashKey = hashString(entry.getKey());
			BigInteger hashValue = hashString(entry.getValue()).subtract(BigInteger.valueOf(-17));
			BigInteger hashEntry = hashKey.xor(hashValue);
			hash = hash.xor(hashEntry);
		}
		return hash;
	}

    /**
     * Hashes a string based on the current configuration.
     * @param input The input string to hash.
     * @return An MD5 or Java hash based on which algorithm has been configured
     */
    private BigInteger hashString(String input) {
		if (algorithm == HashingAlgorithm.JAVA_HASHCODE) {
			return BigInteger.valueOf(input.hashCode());
		} else {
			messageDigest.reset();
			return new BigInteger(1, messageDigest.digest(input.getBytes()));
		}
	}

	public List<Event> process(List<Event> events) {
		LinkedList<Event> intercepted_events = new LinkedList<Event>();

		for (Event event : events) {
			Event intercepted_event = process(event);
			if(intercepted_event != null)
				intercepted_events.add(event);
		}

		int eventsDropped = events.size() - intercepted_events.size();
		if(eventsDropped > 0)
			LOG.debug("Number of events dropped: " + eventsDropped);

		return intercepted_events;
	}

	public void close() {
		previous_hashes.clear();
		hashes_current_batch.clear();
	}

}
