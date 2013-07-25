/*
 * Copyright 2013 David Curtis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.logicmill.util.concurrent;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.logicmill.util.KeyAdapter;
import org.logicmill.util.LargeHashMap;
import org.logicmill.util.LongHashable;
import org.logicmill.util.concurrent.ConcurrentLargeHashMap;

@SuppressWarnings("javadoc")
public class ConcurrentLargeHashMapTest {
	
//	private static String[] keys = new String[0];

	/*
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
		LinkedList<String> words = new LinkedList<String>();
		File rfile = new File("/usr/share/dict/words");
		BufferedReader breader = new BufferedReader(new FileReader(rfile));
		String line = breader.readLine();
		
		while (line != null) {
			words.add(line);
			line = breader.readLine();
		}
		breader.close();		
		keys = words.toArray(keys);
		
		// Fisher-Yates/Knuth shuffle
		Random rng = new Random(1337L);
		for (int i = keys.length - 1; i > 0; i--) {
			int iRnd = rng.nextInt(i+1);
			if (iRnd != i) {
				String tmp = keys[i];
				keys[i] = keys[iRnd];
				keys[iRnd] = tmp;
			}
		}

	}
	*/
	
	@SuppressWarnings("rawtypes")
	/*
	 * Performs a relatively exhaustive integrity check on the internal structure of the map.
	 * See ConcurrentLargeHashMapAuditor for more details.
	 */
	private void checkMapIntegrity(ConcurrentLargeHashMap map) throws SegmentIntegrityException {
        ConcurrentLargeHashMapAuditor auditor = new ConcurrentLargeHashMapAuditor(map);
		LinkedList<SegmentIntegrityException> exceptions = auditor.verifyMapIntegrity(false, 0);
		Assert.assertEquals("map integrity exceptions found", 0, exceptions.size());		
	}
	
	private void printMapStats(ConcurrentLargeHashMap<?,?> map, String title) {
		ConcurrentLargeHashMapProbe mapProbe = new ConcurrentLargeHashMapProbe(map);
		System.out.println();
		System.out.println(title);
		System.out.printf("\tmap size %d%n", map.size());
		System.out.printf("\tsegment size %d%n", mapProbe.getSegmentSize());
		System.out.printf("\tsegment count %d%n", mapProbe.getSegmentCount());
		System.out.printf("\tload factor %f%n", (float)map.size()/((float)( mapProbe.getSegmentCount()*mapProbe.getSegmentSize())) );
		System.out.printf("\tforced splits %d%n", mapProbe.getForcedSplitCount());
		System.out.printf("\tthreshold splits %d%n", mapProbe.getThresholdSplitCount());
	}
	
	public static class ByteKeyAdapter implements KeyAdapter {
		@Override
		public long getLongHashCode(Object key) {
			if (key instanceof byte[]) {
				return org.logicmill.util.hash.SpookyHash64.hash((byte[])key,  0L);							
			} else {
				throw new IllegalArgumentException("key must be type byte[]");
			}
		}
	}
	
	/*
	 * Runs concurrent put test with 8 threads. If the test system has a larger number of cores, consider increasing
	 * the thread count.
	 */
	@Test
	public void testConcurrentPut8() throws SegmentIntegrityException, InterruptedException, ExecutionException {
		ConcurrentLargeHashMap<byte[],Integer> map = new ConcurrentLargeHashMap<byte[],Integer>(8192, 8, 0.9f, new ByteKeyAdapter());
		testConcurrentPut(map, 8, 1000000);
		printMapStats(map, "testConcurrentPut8");
	}

	/*
	 * Runs concurrent put test with load factor 1.0, forcing segment overflow to cause splits.
	 */
	@Test
	public void testConcurrentPutLoad1() throws SegmentIntegrityException, InterruptedException, ExecutionException {
		ConcurrentLargeHashMap<byte[],Integer> map = new ConcurrentLargeHashMap<byte[],Integer>(524288, 2, 1.0f, new ByteKeyAdapter());
		testConcurrentPut(map, 4, 800000);
		printMapStats(map, "testConcurrentPutLoad1");
	}
	
	/*
	 * Runs concurrent put/remove test with 8 threads. If the test system has a larger number of cores, consider increasing
	 * the thread count.
	 */
	@Test
	public void testConcurrentPutRemoveGet8() throws SegmentIntegrityException, InterruptedException, ExecutionException {
		ConcurrentLargeHashMap<byte[],Integer> map = 
				new ConcurrentLargeHashMap<byte[],Integer>(4096, 8, 1.0f, new ByteKeyAdapter());
		testConcurrentPutRemoveGet(map, 8, 200000, 0.25f, 1000000L);
		printMapStats(map, "testConcurrentPutRemoveGet8");
	}


	@Test
	public void testConcurrentPutRemoveGetSmash() throws SegmentIntegrityException, InterruptedException, ExecutionException {
		ConcurrentLargeHashMap<byte[],Integer> map = new ConcurrentLargeHashMap<byte[],Integer>(8192, 2, 1.0f, new ByteKeyAdapter());
		testConcurrentPutRemoveGet(map, 4, 14400, 0.5f, 10000000L);
		printMapStats(map, "testConcurrentPutRemoveGetSmash");
	}

	
	/*
	 * Runs concurrent put/remove/iterator test with 8 threads. If the test system has a larger number of cores, you know
	 * what to do.
	 */
	@Test
	public void testConcurrentPutRemoveIteratorContract8() throws InterruptedException, ExecutionException, SegmentIntegrityException {
		ConcurrentLargeHashMap<byte[],Integer> map = new ConcurrentLargeHashMap<byte[],Integer>(4096, 8, 0.8f, new ByteKeyAdapter());
		testConcurrentPutRemoveIteratorContract(map, 8, 200000, 0.5f);
		printMapStats(map, "testConcurrentPutRemoveIteratorContract8");
	}
	

	@Test
	public void testConcurrentPutRemoveIteratorContract2Segs() throws InterruptedException, ExecutionException, SegmentIntegrityException {
		ConcurrentLargeHashMap<byte[],Integer> map = new ConcurrentLargeHashMap<byte[],Integer>(65536, 2, 1.0f, new ByteKeyAdapter());
		testConcurrentPutRemoveIteratorContract(map, 4, 200000, 0.5f);
		printMapStats(map, "testConcurrentPutRemoveIteratorContract2Segs");
	}
	
	@Test
	public void testConcurrentPutRemoveIterator4() throws InterruptedException, ExecutionException, SegmentIntegrityException {
		ConcurrentLargeHashMap<byte[],Integer> map = new ConcurrentLargeHashMap<byte[],Integer>(65536, 2, 1.0f, new ByteKeyAdapter());
		testConcurrentPutRemoveIterator(map, 4, 200000, 0.5f, 1000000L);
		printMapStats(map, "testConcurrentPutRemoveIterator4");
	}
		
	/* *****************************************************
	 * Simple functional tests with no concurrency. 
	 * *****************************************************
	 */
	

	@Test(expected=NullPointerException.class)
	public void testGetNullKey() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.get(null);		
	}
	
	@Test(expected=NullPointerException.class)
	public void testContainsKeyNullKey() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.containsKey(null);		
	}
	
	
	@Test(expected=NullPointerException.class)
	public void testPutIfAbsentNullKey() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, 
				new KeyAdapter() {
					public long getLongHashCode(Object key) {
						if (key instanceof String) {
							return org.logicmill.util.hash.SpookyHash64.hash((CharSequence)key,  0L);							
						} else {
							throw new IllegalArgumentException("key must be type String");
						}
					}
				}				
			);
		map.putIfAbsent(null, 1);		
	}
	
	@Test(expected=NullPointerException.class)
	public void testPutIfAbsentNullValue() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.putIfAbsent("Hello", null);		
	}

	@Test(expected=NullPointerException.class)
	public void testPutNullKey() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.put(null, 1);		
	}
	
	@Test(expected=NullPointerException.class)
	public void testPutNullValue() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.put("Hello", null);		
	}

	@Test(expected=NullPointerException.class)
	public void testRemoveKNullKey() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.remove(null);				
	}
	

	@Test(expected=NullPointerException.class)
	public void testRemoveKVNullKey() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.remove(null, 1);				
	}

	@Test(expected=NullPointerException.class)
	public void testRemoveKVNullValue() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.remove("Hello", null);				
	}

	@Test(expected=NullPointerException.class)
	public void testReplaceKVNullKey() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.replace(null, 1);				
	}

	@Test(expected=NullPointerException.class)
	public void testReplaceKVNullValue() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.replace("Hello", null);				
	}

	@Test(expected=NullPointerException.class)
	public void testReplaceKVVNullKey() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.replace(null, 1, 2);				
	}

	@Test(expected=NullPointerException.class)
	public void testReplaceKVVNullOldValue() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.replace("Hello", null, 2);				
	}
	
	@Test(expected=NullPointerException.class)
	public void testReplaceKVVNullNullValue() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.replace("Hello", 1, null);				
	}
	
	
	
	@Test
	public void testDefaultKeyAdapterString() throws SegmentIntegrityException {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		map.put("hello", 1);
		Assert.assertTrue(map.containsKey("hello"));
	}
		
	public class LongHashableString implements LongHashable {
		
		private final String string;
		
		public LongHashableString(String s) {
			string = s;
		}

		@Override
		public long getLongHashCode() {
			return org.logicmill.util.hash.SpookyHash64.hash(string, 0L);
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof LongHashableString) {
				LongHashableString other = (LongHashableString) obj;
				return this.string.equals(other.string);
			} else {
				return false;
			}

		}
		
	}
	
	@Test
	public void testDefaultKeyAdapterLongHashable() throws SegmentIntegrityException {
		final ConcurrentLargeHashMap<LongHashableString, Integer> map = 
				new ConcurrentLargeHashMap<LongHashableString, Integer>(1024, 2, 0.8f, null);
		map.putIfAbsent(new LongHashableString("hello"), new Integer(0));
		Integer n = map.get(new LongHashableString("hel"+"lo"));
		Assert.assertNotNull(n);
		Assert.assertEquals(0, n.intValue());
        checkMapIntegrity(map);	
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testDefaultKeyAdapterTypeMismatch()  {
		final ConcurrentLargeHashMap<Long, Integer> map = 
			new ConcurrentLargeHashMap<Long, Integer>(1024, 2, 0.8f, null);
		map.putIfAbsent(new Long(0L), new Integer(0));
	}

	/*
	 * Confirms that remove(key, value) only removes the entry if the key maps to the specificed value.
	 */
	@Test
	public void testRemoveValue() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		String key = "Hello";
		map.put(key, 1);
		Assert.assertEquals(map.size(), 1);
		Assert.assertEquals(true, map.containsKey(key));
		Assert.assertTrue(map.remove(key, 1));
		Assert.assertEquals(false, map.containsKey(key));
		map.put(key, 2);
		Assert.assertEquals(true, map.containsKey(key));
		Assert.assertFalse(map.remove(key, 1));
		Assert.assertEquals(true, map.containsKey(key));
		Assert.assertEquals(2, map.get(key).intValue());
	}
	

	@Test
	public void testRemoveValueAll() throws SegmentIntegrityException {
		RandomKeySet keySet = new RandomKeySet(100000, 128, 1337L);
		final ConcurrentLargeHashMap<byte[], Integer> map = 
				new ConcurrentLargeHashMap<byte[], Integer>(1024, 2, 0.8f, new ByteKeyAdapter());
		for (int i = 0; i < keySet.size(); i++) {
			map.putIfAbsent(keySet.getKey(i), new Integer(i));
		}
		for (int i = 0; i < keySet.size(); i++) {
			Assert.assertFalse(map.remove(keySet.getKey(i), new Integer(-1)));
		}
		for (int i = 0; i < keySet.size(); i++) {
			Assert.assertTrue(map.remove(keySet.getKey(i), new Integer(i)));
		}
        checkMapIntegrity(map);	
		
	}

	
	/*
	 * Confirms that containsKey(key) and remove(key) function properly.
	 */
	@Test
	public void testContainsKeyAndRemove() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		String key = "Hello";
		map.put(key, 1);
		Assert.assertEquals(map.size(), 1);
		Assert.assertEquals(true, map.containsKey(key));
		Integer val = map.remove(key);
		Assert.assertEquals(1, val.intValue());
		Assert.assertEquals(false, map.containsKey(key));
	}
	
	/*
	 * Confirms that get(key) functions properly.
	 */
	@Test
	public void testGet() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		String key = "Hello";
		map.put(key, 1);
		Assert.assertEquals(map.size(), 1);
		Assert.assertEquals(true, map.containsKey(key));
		Integer val = map.get(key);
		Assert.assertEquals(1, val.intValue());
	}

	/*
	 * Confirms that isEmpty() functions properly.
	 */
	@Test
	public void testIsEmpty() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		Assert.assertTrue(map.isEmpty());
		String key = "Hello";
		map.put(key, 1);
		Assert.assertFalse(map.isEmpty());
	}

	/*
	 * Confirms that replace(key, value) functions properly; specifically, that it only
	 * replaces if a mapping for key already exists in the map, and that the return
	 * value matches the previously existing mapped value, or null if no previous mapping
	 * existed.
	 */
	@Test
	public void testReplace() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		String key = "Hello";
		map.put(key, 1);
		Assert.assertEquals(map.size(), 1);
		Assert.assertEquals(true, map.containsKey(key));
		Integer val = map.get(key);
		Assert.assertEquals(1, val.intValue());
		Integer oldValue = map.replace(key, 2);
		Assert.assertNotNull(oldValue);
		Assert.assertEquals(1, oldValue.intValue());
		Integer newValue = map.get(key);
		Assert.assertEquals(2, newValue.intValue());
		newValue = map.remove(key);
		Assert.assertEquals(2, newValue.intValue());
		newValue = map.replace(key, 3);
		Assert.assertNull(newValue);
		Assert.assertFalse(map.containsKey(key));
	}
	
	@Test
	public void testReplaceAll() throws SegmentIntegrityException {
		RandomKeySet keySet = new RandomKeySet(100000, 128, 1337L);
		final ConcurrentLargeHashMap<byte[], Integer> map = 
				new ConcurrentLargeHashMap<byte[], Integer>(1024, 2, 0.8f, new ByteKeyAdapter());	
		for (int i = 0; i < keySet.size(); i++) {
			map.putIfAbsent(keySet.getKey(i), new Integer(i));
		}
		for (int i = 0; i < keySet.size(); i++) {
			Integer n = map.replace(keySet.getKey(i), new Integer(-1));
			Assert.assertNotNull(n);
			Assert.assertEquals(i, n.intValue());
		}
        checkMapIntegrity(map);	

	}

	@Test
	public void testPutWithReplace() throws SegmentIntegrityException {
		RandomKeySet keySet = new RandomKeySet(100000, 128, 1337L);
		final ConcurrentLargeHashMap<byte[], Integer> map = 
				new ConcurrentLargeHashMap<byte[], Integer>(1024, 2, 0.8f, new ByteKeyAdapter());	
		for (int i = 0; i < keySet.size(); i++) {
			map.putIfAbsent(keySet.getKey(i), new Integer(i));
		}
		for (int i = 0; i < keySet.size(); i++) {
			Integer n = map.put(keySet.getKey(i), new Integer(-1));
			Assert.assertNotNull(n);
			Assert.assertEquals(i, n.intValue());
		}
        checkMapIntegrity(map);	

	}


	
	/*
	 * Confirms that replace(key, oldValue, newValue) functions properly; specifically, that replacement
	 * occurs only if a previous mapping for key exists in the map, and the previous mapped value 
	 * is equal to oldValue. Also confirms that the correct newValue is mapped (when appropriate) and 
	 * that the return boolean value is correct.
	 */
	@Test
	public void testReplaceOldNew() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		String key = "Hello";
		map.put(key, 1);
		Assert.assertEquals(map.size(), 1);
		Assert.assertEquals(true, map.containsKey(key));
		Integer val = map.get(key);
		Assert.assertEquals(1, val.intValue());
		Assert.assertTrue(map.replace(key, 1, 2));
		Assert.assertEquals(2, map.get(key).intValue());
		Assert.assertFalse(map.replace(key, 1, 3));
		Assert.assertEquals(2, map.get(key).intValue());
		Integer value = map.replace(key, 3);
		Assert.assertNotNull(value);
		Assert.assertEquals(2, value.intValue());
		value = map.remove(key);
		Assert.assertEquals(3, value.intValue());
		Assert.assertFalse(map.replace(key, 3, 4));
		Assert.assertFalse(map.containsKey(key));
	}

	/*
	 * Confirms that putIfAbsent(key, value) functions correctly; specifically, that the mapping
	 * occurs only if no previous mapping for key exists in the map. Also confirms that the 
	 * return value is null if no mapping previously existed, or that the return value matches
	 * the value to which key previously mapped (in which case the put does not change the mapping).
	 */
	@Test
	public void testPutIfAbsent() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(1024, 2, 0.8f, null);
		String key = "Hello";
		Assert.assertNull(map.putIfAbsent(key, 1));
		Assert.assertEquals(map.size(), 1);
		Assert.assertEquals(true, map.containsKey(key));
		Integer value = map.get(key);
		Assert.assertEquals(1, value.intValue());
		value = map.putIfAbsent(key,2);
		Assert.assertEquals(1, value.intValue());
		value = map.get(key);
		Assert.assertEquals(1, value.intValue());
	}
	
	
	/*
	 * Confirms that the iterator returned by getKeyIterator() functions properly; specifically, that
	 * all keys in the map are returned by the iterator, and that hasNext() returns true until the
	 * iterator is exhausted, and false afterward.
	 */
	@Test
	public void testKeyIterator() {
		RandomKeySet keySet = new RandomKeySet(100000, 128, 1337L);
		final ConcurrentLargeHashMap<byte[], Integer> map = 
				new ConcurrentLargeHashMap<byte[], Integer>(4096, 8, 0.8f, new ByteKeyAdapter());
		HashSet<byte[]> intoMap = new HashSet<byte[]>();
		int i = 0;
		while (keySet.hasMoreKeys()) {
			byte[] key = keySet.getKey();
			intoMap.add(key);
			map.put(key, i++);
		}
		HashSet<byte[]> fromMap = new HashSet<byte[]>();
		Iterator<byte[]> keyIter = map.getKeyIterator();
		while (keyIter.hasNext()) {
			fromMap.add(keyIter.next());
		}
		Assert.assertEquals(intoMap, fromMap);
	}
	
	/*
	 * Confirms that the KeyIterator.remove() method throws an UnsupportedOperationException
	 */
	@Test(expected=UnsupportedOperationException.class)
	public void testKeyIteratorRemove() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(4096, 8, 0.8f, null);
		map.put("Hello", 1);
		Iterator<String> keyIter = map.getKeyIterator();
		Assert.assertTrue(keyIter.hasNext());
		String key = keyIter.next();
		Assert.assertEquals("Hello", key);
		keyIter.remove();
	}
	
	/*
	 * Confirms that calling KeyIterator.next() after exhaustion throws a NoSuchElementException
	 */
	@Test(expected=NoSuchElementException.class)
	public void testKeyIteratorExhaustion() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(4096, 8, 0.8f, null);
		map.put("Hello", 1);
		Iterator<String> keyIter = map.getKeyIterator();
		@SuppressWarnings("unused")
		String key = keyIter.next();
		key = keyIter.next();
	}



	/*
	 * Confirms that the iterator returned by getEntryIterator() functions properly; specifically, that
	 * all entries in the map are returned by the iterator, and that hasNext() returns true until the
	 * iterator is exhausted, and false afterward.
	 */
	@Test
	public void testEntryIterator() {
		RandomKeySet keySet = new RandomKeySet(100000, 128, 1337L);
		final ConcurrentLargeHashMap<byte[], Integer> map = 
				new ConcurrentLargeHashMap<byte[], Integer>(4096, 8, 0.8f, new ByteKeyAdapter());
		HashSet<byte[]> keysIntoMap = new HashSet<byte[]>();
		HashSet<Integer> valsIntoMap = new HashSet<Integer>();
		int i = 0;
		while (keySet.hasMoreKeys()) {
			byte[] key = keySet.getKey();
			keysIntoMap.add(key);
			valsIntoMap.add(i);
			map.put(key, i++);
		}
		
		HashSet<Integer> valsFromMap = new HashSet<Integer>();
		HashSet<byte[]> keysFromMap = new HashSet<byte[]>();
		Iterator<LargeHashMap.Entry<byte[], Integer>> entryIter = map.getEntryIterator();
		while (entryIter.hasNext()) {
			LargeHashMap.Entry<byte[],Integer> entry = entryIter.next();
			valsFromMap.add(entry.getValue());
			keysFromMap.add(entry.getKey());
		}
		Assert.assertEquals(valsIntoMap, valsFromMap);
		Assert.assertEquals(keysIntoMap, keysFromMap);
	}
	
	/*
	 * Confirms that the EntryIterator.remove() method throws an UnsupportedOperationException
	 */
	@Test(expected=UnsupportedOperationException.class)
	public void testEntryIteratorRemove() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(4096, 8, 0.8f, null);
		map.put("Hello", 1);
		Iterator<LargeHashMap.Entry<String, Integer>> entryIter = map.getEntryIterator();
		@SuppressWarnings("unused")
		LargeHashMap.Entry<String, Integer> entry = entryIter.next();
		entryIter.remove();
	}

	/*
	 * Confirms that calling EntryIterator.next() after exhaustion throws a NoSuchElementException
	 */
	@Test(expected=NoSuchElementException.class)
	public void testEntryIteratorExhaustion() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(4096, 8, 0.8f, null);
		map.put("Hello", 1);
		Iterator<LargeHashMap.Entry<String, Integer>> entryIter = map.getEntryIterator();
		@SuppressWarnings("unused")
		LargeHashMap.Entry<String, Integer> entry = entryIter.next();
		entry = entryIter.next();
	}

	
	/*
	 * Confirms that the iterator returned by getValueIterator() functions properly; specifically, that
	 * all values in the map are returned by the iterator, and that hasNext() returns true until the
	 * iterator is exhausted, and false afterward.
	 */
	@Test
	public void testValueIterator() {
		RandomKeySet keySet = new RandomKeySet(100000, 128, 1337L);
		final ConcurrentLargeHashMap<byte[], Integer> map = 
				new ConcurrentLargeHashMap<byte[], Integer>(4096, 8, 0.8f, new ByteKeyAdapter());
		HashSet<Integer> intoMap = new HashSet<Integer>();
		int i = 0;
		while(keySet.hasMoreKeys()) {
			intoMap.add(i);
			map.put(keySet.getKey(), i++);
		}
		HashSet<Integer> fromMap = new HashSet<Integer>();
		Iterator<Integer> valIter = map.getValueIterator();
		while (valIter.hasNext()) {
			fromMap.add(valIter.next());
		}
		Assert.assertEquals(intoMap, fromMap);		
	}

	/*
	 * Confirms that the ValueIterator.remove() method throws an UnsupportedOperationException
	 */
	@Test(expected=UnsupportedOperationException.class)
	public void testValueIteratorRemove() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(4096, 8, 0.8f, null);
		map.put("Hello", 1);
		Iterator<Integer> valueIter = map.getValueIterator();
		@SuppressWarnings("unused")
		Integer value = valueIter.next();
		valueIter.remove();
	}

	/*
	 * Confirms that calling ValueIterator.next() after exhaustion throws a NoSuchElementException
	 */
	@Test(expected=NoSuchElementException.class)
	public void testValueIteratorExhaustion() {
		final ConcurrentLargeHashMap<String, Integer> map = new ConcurrentLargeHashMap<String, Integer>(4096, 8, 0.8f, null);
		map.put("Hello", 1);
		Iterator<Integer> valueIter = map.getValueIterator();
		@SuppressWarnings("unused")
		Integer value = valueIter.next();
		value = valueIter.next();
	}


	
	/*
	 * Configurable stress test of concurrent put/remove/get operations.
	 * 
	 * For convenience, key/value pairs always consist of (keys[i], i), so a value
	 * can always be reverse-mapped to the corresponding key through the keys array.
	 * This test populates the map with a fraction of the available keys, based on 
	 * the removeDepth parameter. If removeDepth >= 0, the map is filled with 
	 * keys.length - removeDepth keys; if removeDepth < 0, the map is filled with 
	 * keys.length/2 keys. The unused keys are put in a linked queue (the recycle queue). When the concurrent
	 * test starts, threadCount-1 threads execute the recycleTask callable, which alternates
	 * between two actions: 1) get a key from the recycle queue and put it in the map, and 2)
	 * select a random entry in the map and remove it, placing it in the recycle queue. This
	 * guarantees a sustainable flow on entries in and out of the map, with a (roughly) fixed
	 * fraction of keys in the map. The recycleTask threads continue until a total of recycleLimit
	 * put/remove operations have completed. The remaining thread executes the
	 * getTask callable, which performs get() operations on random keys, confirming that
	 * the entrys (keys[i], i) are valid. After the completion of the concurrent threads,
	 * the map integrity is checked.
	 * 
	 * 
	 * @param threadCount number of threads employed in test
	 * @param removeDepth if >= 0, the number of keys held in the 
	 * recycle queue; if -1, half of the available keys are held in the recycle queue
	 * @param recycleLimit maximum number of put/remove operations performed in the test
	 * @param segSize segment size for the test map
	 * @param segCount initial segment count for the test map
	 * @param loadFactor load factor threshold for the test map
	 * @throws SegmentIntegrityException 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private void testConcurrentPutRemoveGet(final ConcurrentLargeHashMap<byte[], Integer> map,
			final int threadCount, final int keyCount, final float recycleFraction, final long recycleLimit)
			throws SegmentIntegrityException, InterruptedException, ExecutionException {
		
		final RandomKeySet keySet = new RandomKeySet(keyCount, 128, 1337L);

		final ConcurrentLinkedQueue<Integer> recycleQueue = new ConcurrentLinkedQueue<Integer>();
		
		int recycQueueSize = (int)(keyCount * recycleFraction);
		
		for (int i = 0; i < keyCount; i++) {
			map.putIfAbsent(keySet.getKey(i), new Integer(i));
		}
		
		Random rng = new Random(1337L);
		for (int i = 0; i < recycQueueSize; i++) {
			int ri = rng.nextInt(keyCount);
			byte[] key = keySet.getKey(ri);
			Integer val = map.remove(key);
			while (val == null) {
				ri = rng.nextInt(keyCount);
				key = keySet.getKey(ri);
				val = map.remove(key);
			}
			recycleQueue.offer(val);
		}
		
		final AtomicLong recycleCount = new AtomicLong(0L);
		
		Callable<Long> recycleTask = new Callable<Long>() {
			@Override
			public Long call() throws InterruptedException {
				long totalRecycles = recycleCount.getAndIncrement();
				long localRecycleCount = 0L;
				long currentThreadID = Thread.currentThread().getId();
				Random localRng = new Random(currentThreadID);
				while (totalRecycles < recycleLimit) {
					Integer recycleVal = recycleQueue.poll();
					Assert.assertNotNull(recycleVal);
					Integer inMap = map.putIfAbsent(keySet.getKey(recycleVal.intValue()), recycleVal);
					Assert.assertNull("unexpected recycle value already in map", inMap);
					
					int ri = localRng.nextInt(keyCount);
					Integer removedVal = map.remove(keySet.getKey(ri));
					while (removedVal == null) {
						ri = localRng.nextInt(keyCount);
						removedVal = map.remove(keySet.getKey(ri));
					}
					Assert.assertEquals("removed value mismatch", removedVal.intValue(), ri);
					recycleQueue.offer(removedVal);
					localRecycleCount++;
					totalRecycles = recycleCount.getAndIncrement();
				}
				
				Integer val = recycleQueue.poll();
				while (val != null) {
					Integer inMap = map.putIfAbsent(keySet.getKey(val.intValue()), val);
					Assert.assertNull("unexpected recycle value already in map", inMap);	
					val = recycleQueue.poll();
				}
				
				return localRecycleCount;
			}
		};
		
		Callable<Long> getTask = new Callable<Long>() {
			@Override
			public Long call() throws InterruptedException {
				long totalGets = 0L;
				long currentThreadID = Thread.currentThread().getId();
				Random localRng = new Random(currentThreadID);
				while (recycleCount.get() < recycleLimit) {
					int ri = localRng.nextInt(keyCount);
					Integer val = map.get(keySet.getKey(ri));
					if (val != null) {
						Assert.assertEquals(ri, val.intValue());
					}
					totalGets++;
				}
				return totalGets;
			}
		};
		
		List<Callable<Long>> recycleTasks = Collections.nCopies(threadCount-1, recycleTask);		
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        LinkedList<Future<Long>> results = new LinkedList<Future<Long>>();
        for (Callable<Long> task : recycleTasks) {
            results.add(executorService.submit(task));
        }
        Future<Long> getResult = executorService.submit(getTask);
        long totalRecycles = 0L;
        for (Future<Long> result : results) {
        	totalRecycles += result.get();
        }
        long totalGets = getResult.get();

		System.out.printf("totalGets %d%n", totalGets);
		
		Assert.assertEquals("total recycles/recycleLimit mismatch", totalRecycles, recycleLimit);
		Assert.assertEquals("keyCount/map size mismatch", keyCount, map.size());
        for (int i = 0; i < keyCount; i++) {
        	Integer val = map.get(keySet.getKey(i));
        	Assert.assertNotNull(String.format("entry %d missing from map", i), val);
        	Assert.assertEquals("wrong value for key in map", val.intValue(), i);
        }
        checkMapIntegrity(map);
	}
	
	@Test
	public void testConcurrentGet4() throws SegmentIntegrityException, InterruptedException, ExecutionException {
		ConcurrentLargeHashMap<byte[], Integer> map = 
				new ConcurrentLargeHashMap<byte[], Integer>(8192, 4, 0.8f, new ByteKeyAdapter());
		testConcurrentGet(map, 4, 200000, 5000000L);
	}
	
	private void testConcurrentGet(final ConcurrentLargeHashMap<byte[], Integer> map, int threadCount, final int keyCount, final long getLimit)
			throws SegmentIntegrityException, InterruptedException, ExecutionException {

		final RandomKeySet keySet = new RandomKeySet(keyCount, 128, 1337L);
		
		Callable<Long> getTask = new Callable<Long>() {
			@Override
			public Long call() {
				int startIndex = 0;
				long currentThreadID = Thread.currentThread().getId();
				if (currentThreadID < 0) {
					currentThreadID &= (1L << 62) - 1L;
				}
				startIndex = (int)(currentThreadID % keySet.size());
				long getCount = 0;
				int next = startIndex; 
				while (getCount++ < getLimit) {
					map.get(keySet.getKey(next));					
					if (++next >= keySet.size()) {
						next = 0;
					}
				}
				return getCount;
			}
		};
		
		
		for (int i = 0; i < keySet.size(); i++) {
			map.putIfAbsent(keySet.getKey(i), new Integer(i));
		}

		List<Callable<Long>> getTasks = Collections.nCopies(threadCount, getTask);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<Long>> futures = executorService.invokeAll(getTasks);
        Assert.assertEquals("future count should equal threadCount", threadCount, futures.size());
        // Check for exceptions
        long totalGets = 0;
        for (Future<Long> future : futures) {
            // Throws an exception if an exception was thrown by the task.
            totalGets += future.get();
        }
        System.out.printf("total gets: %d%n", totalGets);

	}
	
	/* Creates threadCount threads that concurrently put (keys[i],i) entries for all elements
	 * in keys. When keys are exhausted, the threads terminate, and the main thread performs
	 * a map integrity check.
	 * 
	 * @param threadCount number of threads concurrently adding to the map
	 * @param segSize size of segments in the created map
	 * @param segCount number of segments initially created in the map
	 * @param loadFactor load factory threshold for the map
	 * @throws SegmentIntegrityException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private void testConcurrentPut(final ConcurrentLargeHashMap<byte[], Integer> map, int threadCount, int keyCount)
			throws SegmentIntegrityException, InterruptedException, ExecutionException {

		final RandomKeySet keySet = new RandomKeySet(keyCount, 128, 1337L);

		final AtomicInteger nextKey = new AtomicInteger(0);
		
		Callable<Integer> putTask = new Callable<Integer>() {
			@Override
			public Integer call() {
				int putCount = 0;
				int next = nextKey.getAndIncrement(); 
				while (next < keySet.size()) {
					byte[] key = keySet.getKey(next);
					Integer val = new Integer(next);
					Integer inMap = map.putIfAbsent(key, val);
					Assert.assertNull(inMap);
					putCount++;
					next = nextKey.getAndIncrement();
				}
				return putCount;
			}
		};
		
		List<Callable<Integer>> putTasks = Collections.nCopies(threadCount, putTask);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> futures = executorService.invokeAll(putTasks);
        Assert.assertEquals("future count should equal threadCount", threadCount, futures.size());
        // Check for exceptions
        int totalPuts = 0;
        for (Future<Integer> future : futures) {
            // Throws an exception if an exception was thrown by the task.
            totalPuts += future.get();
        }
        Assert.assertEquals("keyCount/totalPuts mismatch", keySet.size(), totalPuts);
        Assert.assertEquals("keyCount/map size mismatch", keySet.size(), map.size());
        for (int i = 0; i < keySet.size(); i++) {
        	Integer val = map.get(keySet.getKey(i));
        	Assert.assertNotNull(String.format("entry %d missing from map", i), val);
        	Assert.assertEquals("wrong value for key in map", val.intValue(), i);
        }
        checkMapIntegrity(map);
	}
	
	
	/* Concurrently executes put, remove, and iteration.
	 * 
	 * Creates threadCount - 1 threads that execute the 
	 * @param threadCount
	 * @param absentKeyPoolSize
	 * @param segSize
	 * @param segCount
	 * @param loadFactor
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws SegmentIntegrityException
	 */
	private void testConcurrentPutRemoveIteratorContract(final ConcurrentLargeHashMap<byte[], Integer> map, 
			int threadCount, final int keyCount, float fractionInPool) 
	throws InterruptedException, ExecutionException, SegmentIntegrityException {
		final ConcurrentLinkedQueue<byte[]> initallyInMap = new ConcurrentLinkedQueue<byte[]>();
		final ConcurrentLinkedQueue<byte[]> initiallyNotInMap = new ConcurrentLinkedQueue<byte[]>();
		final ConcurrentLinkedQueue<byte[]> removedFromMap = new ConcurrentLinkedQueue<byte[]>();
		final ConcurrentLinkedQueue<byte[]> putInMap = new ConcurrentLinkedQueue<byte[]>();
		final AtomicBoolean iteratorStarted = new AtomicBoolean(false);
		final AtomicBoolean iteratorFinished = new AtomicBoolean(false);
		
		final RandomKeySet keySet = new RandomKeySet(keyCount, 128, 1337L);

		int notInMapCount = (int)(keyCount * fractionInPool);
		
		for (int i = 0; i < keySet.size(); i++) {
			if (i < notInMapCount) {
				initiallyNotInMap.add(keySet.getKey(i));
			} else {
				map.putIfAbsent(keySet.getKey(i), new Integer(i));
				initallyInMap.add(keySet.getKey(i));
			}
		}
		int initiallyPresent = initallyInMap.size();
		int initiallyAbsent = initiallyNotInMap.size();
		
		Callable<Integer> putRemoveTask = new Callable<Integer>() {
			@Override
			public Integer call() throws InterruptedException {
				int removed = 0;
				byte[] removeKey = initallyInMap.poll();
				while (removeKey != null) {
					if (iteratorStarted.get()) {
						if (!iteratorFinished.get()) {
							/*
							 * The iterator is in use
							 */
							Assert.assertNotNull(map.remove(removeKey)); // remove key from the map
							removedFromMap.add(removeKey); // put it in removedFromMap
							byte[] addKey = initiallyNotInMap.poll(); // get a key that's not in the map
							if (addKey != null) {
								Assert.assertNull(map.putIfAbsent(addKey,-1));
								putInMap.add(addKey); // put it in the map
							}
							removed++;
						} else {
							/*
							 * if the iterator is exhausted, put the key back and terminate
							 */
							initallyInMap.add(removeKey);
							break;
						}
					} else {
						/*
						 * the iterator hasn't started yet, so put the key back
						 */
						initallyInMap.add(removeKey);
					}
					removeKey = initallyInMap.poll(); // get the next key to remove
				}
				return new Integer(removed);
			}
		};
		
		Callable<LinkedList<byte[]>> iteratorTask = new Callable<LinkedList<byte[]>>() {
			@Override
			public LinkedList<byte[]> call() throws InterruptedException {
				LinkedList<byte[]> observed = new LinkedList<byte[]>();
				Iterator<byte[]> keyIter = map.getKeyIterator();
				iteratorStarted.set(true);
				while (keyIter.hasNext()) {
					observed.add(keyIter.next());
				}
				iteratorFinished.set(true);
				return observed;
			}
		};
	
		List<Callable<Integer>> putRemoveTasks = Collections.nCopies(threadCount-1, putRemoveTask);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        LinkedList<Future<Integer>> results = new LinkedList<Future<Integer>>();
        for (Callable<Integer> task : putRemoveTasks) {
            results.add(executorService.submit(task));
        }
        Future<LinkedList<byte[]>> iteratorResult = executorService.submit(iteratorTask);
        @SuppressWarnings("unused")
		int totalRemoves = 0;
        for (Future<Integer> result : results) {
        	totalRemoves += result.get().intValue();
        }
        LinkedList<byte[]> observed = iteratorResult.get();
        HashSet<byte[]> observedSet = new HashSet<byte[]>(observed);

        int finallyPresent = initallyInMap.size();
        int finallyAbsent = initiallyNotInMap.size();
        int added = putInMap.size();
        int removed = removedFromMap.size();
        int addedAndObserved = 0;
        for (byte[] key : putInMap) {
        	if (observedSet.contains(key)) {
        		addedAndObserved++;
        	}
        }
        int removedAndObserved = 0;
        for (byte[] key : removedFromMap) {
        	if (observedSet.contains(key)) {
        		removedAndObserved++;
        	}
        }
        Assert.assertTrue(observedSet.containsAll(initallyInMap));
        for (byte[] key : initiallyNotInMap) {
        	Assert.assertFalse(observedSet.contains(key));
        }
        Assert.assertEquals(finallyPresent + putInMap.size(), map.size());
        checkMapIntegrity(map);

        
        System.out.printf("initiallyPresent %d, initiallyAbsent %d, finallyPresent %d, finallyAbsent %d, added %d, removed %d%n", 
        		initiallyPresent, initiallyAbsent, finallyPresent, finallyAbsent, added, removed);
        System.out.printf("addedAndObserved %d, removedAndObserved %d, observed %d, map size %d%n", 
        		addedAndObserved, removedAndObserved, observed.size(), map.size() );
	}
	
	private void testConcurrentPutRemoveIterator(final ConcurrentLargeHashMap<byte[], Integer> map,
			final int threadCount, final int keyCount, final float recycleFraction, final long recycleLimit)
			throws SegmentIntegrityException, InterruptedException, ExecutionException {
		
		final RandomKeySet keySet = new RandomKeySet(keyCount, 128, 1337L);
		final ConcurrentLinkedQueue<Integer> recycleQueue = new ConcurrentLinkedQueue<Integer>();
		
		int recycQueueSize = (int)(keyCount * recycleFraction);
		
		for (int i = 0; i < keyCount; i++) {
			map.putIfAbsent(keySet.getKey(i), new Integer(i));
		}
		
		Random rng = new Random(1337L);
		for (int i = 0; i < recycQueueSize; i++) {
			int ri = rng.nextInt(keyCount);
			byte[] key = keySet.getKey(ri);
			Integer val = map.remove(key);
			while (val == null) {
				ri = rng.nextInt(keyCount);
				key = keySet.getKey(ri);
				val = map.remove(key);
			}
			recycleQueue.offer(val);
		}
		
		final AtomicLong recycleCount = new AtomicLong(0L);
		
		Callable<Long> recycleTask = new Callable<Long>() {
			@Override
			public Long call() throws InterruptedException {
				long totalRecycles = recycleCount.getAndIncrement();
				long localRecycleCount = 0L;
				long currentThreadID = Thread.currentThread().getId();
				Random localRng = new Random(currentThreadID);
				while (totalRecycles < recycleLimit) {
					Integer recycleVal = recycleQueue.poll();
					Assert.assertNotNull(recycleVal);
					Integer inMap = map.putIfAbsent(keySet.getKey(recycleVal.intValue()), recycleVal);
					Assert.assertNull("unexpected recycle value already in map", inMap);
					
					int ri = localRng.nextInt(keyCount);
					Integer removedVal = map.remove(keySet.getKey(ri));
					while (removedVal == null) {
						ri = localRng.nextInt(keyCount);
						removedVal = map.remove(keySet.getKey(ri));
					}
					Assert.assertEquals("removed value mismatch", removedVal.intValue(), ri);
					recycleQueue.offer(removedVal);
					localRecycleCount++;
					totalRecycles = recycleCount.getAndIncrement();
				}
				
				Integer val = recycleQueue.poll();
				while (val != null) {
					Integer inMap = map.putIfAbsent(keySet.getKey(val.intValue()), val);
					Assert.assertNull("unexpected recycle value already in map", inMap);	
					val = recycleQueue.poll();
				}
				
				return localRecycleCount;
			}
		};
		
		Callable<Long> iteratorTask = new Callable<Long>() {
			@Override
			public Long call() throws InterruptedException {
				long localIterations = 0L;
				while (recycleCount.get() < recycleLimit) {
					Iterator<LargeHashMap.Entry<byte[],Integer>> iter = map.getEntryIterator();
					while (iter.hasNext()) {
						@SuppressWarnings("unused")
						LargeHashMap.Entry<byte[],Integer> entry = iter.next();
					}
					localIterations++;
				}
				return localIterations;
			}
		};
		
		List<Callable<Long>> recycleTasks = Collections.nCopies(threadCount-1, recycleTask);		
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        LinkedList<Future<Long>> results = new LinkedList<Future<Long>>();
        for (Callable<Long> task : recycleTasks) {
            results.add(executorService.submit(task));
        }
        Future<Long> getResult = executorService.submit(iteratorTask);
        long totalRecycles = 0L;
        for (Future<Long> result : results) {
        	totalRecycles += result.get();
        }
        long totalIterations = getResult.get();

		System.out.printf("totalIterations %d%n", totalIterations);
		
		Assert.assertEquals("total recycles/recycleLimit mismatch", totalRecycles, recycleLimit);
		Assert.assertEquals("keyCount/map size mismatch", keyCount, map.size());
        for (int i = 0; i < keyCount; i++) {
        	Integer val = map.get(keySet.getKey(i));
        	Assert.assertNotNull(String.format("entry %d missing from map", i), val);
        	Assert.assertEquals("wrong value for key in map", val.intValue(), i);
        }
        checkMapIntegrity(map);
	}


}
