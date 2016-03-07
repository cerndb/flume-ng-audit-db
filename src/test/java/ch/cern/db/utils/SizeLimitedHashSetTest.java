package ch.cern.db.utils;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;

/**
 * It acts as a queue, removing last element inserted when Set reach maximun size.
 * 
 * @author daniellanzagarcia
 *
 * @param <E>
 */
@SuppressWarnings("serial")
public class SizeLimitedHashSetTest<E> extends HashSet<E> {

	@Test
	public void duplicateElements(){
		SizeLimitedHashSet<Integer> set = new SizeLimitedHashSet<>(10);
		
		set.add(1);
		set.add(1);
		set.add(2);
		
		Assert.assertEquals(2, set.size());
		Assert.assertTrue(set.contains(1));
		Assert.assertTrue(set.contains(2));
	}
	
	@Test
	public void reachMaximunSize(){
		SizeLimitedHashSet<Integer> set = new SizeLimitedHashSet<>(3);
		
		set.add(1);
		set.add(2);
		set.add(3);
		
		Assert.assertEquals(3, set.size());
		Assert.assertTrue(set.contains(1));
		Assert.assertTrue(set.contains(2));
		Assert.assertTrue(set.contains(3));
		
		set.add(4);
		set.add(5);
		
		Assert.assertEquals(3, set.size());
		Assert.assertTrue(set.contains(3));
		Assert.assertTrue(set.contains(4));
		Assert.assertTrue(set.contains(5));
		
		set.add(1);
		
		Assert.assertEquals(3, set.size());
		Assert.assertTrue(set.contains(4));
		Assert.assertTrue(set.contains(5));
		Assert.assertTrue(set.contains(1));
		
		set.addAll(Arrays.asList(6, 7));
		
		Assert.assertEquals(3, set.size());
		Assert.assertTrue(set.contains(1));
		Assert.assertTrue(set.contains(6));
		Assert.assertTrue(set.contains(7));
	}
	
}
