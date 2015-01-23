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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.logicmill.util.LargeHashMap;
import org.logicmill.util.concurrent.ConcurrentHashMap;
import org.logicmill.util.concurrent.ConcurrentLargeHashMapProbe.ProbeInternalException;
import org.logicmill.util.concurrent.ConcurrentLargeHashMapProbe.SegmentProbe;

/** An object that verifies the integrity of an instance of 
 * {@link ConcurrentHashMap}. This class uses 
 * {@link ConcurrentLargeHashMapProbe}
 * to gain access to private members of {@code ConcurrentHashMap}.
 * 
 * To audit an instance of {@code ConcurrentHashMap}:<pre><code>
 * ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap( ... );
 * ConcurrentLargeHashMapAuditor auditor = new ConcurrentLargeHashMapAuditor(map);
 * ...
 * try {
 * 	auditor.verifyMapIntegrity(true, 0);
 * } catch (SegmentIntegrityException e) {
 * 	...
 * }
 * </code></pre>
 * 
 * @see SegmentIntegrityException
 * @see BucketOrderException
 * @see EntryCountException
 * @see EntryNotReachableException
 * @see IllegalBucketValueException
 * @see IllegalOffsetValueException
 * @see NullEntryInBucketException
 * @see WrongBucketException
 * @see WrongSegmentException
 * @see ConcurrentLargeHashMapProbe
 * @author David Curtis
 *
 */
public class ConcurrentLargeHashMapAuditor {
	
	private class ExceptionLimitReachedException extends Exception {
		private static final long serialVersionUID = -6214653529057631843L;
	}
	
	private class ExceptionAccumulator {

		private final boolean throwImmediately;
		private final int maxExceptions;
		private final LinkedList<SegmentIntegrityException> exceptions;
		
		private ExceptionAccumulator(boolean throwImmediately, int maxExceptions) {
			this.throwImmediately = throwImmediately;
			this.maxExceptions = maxExceptions;
			if (!throwImmediately) {
				exceptions = new LinkedList<SegmentIntegrityException>();
			} else {
				exceptions = null;
			}
		}
		
		private void noteException(SegmentIntegrityException e) 
		throws ExceptionLimitReachedException, SegmentIntegrityException {
			if (throwImmediately) {
				throw e;
			} else {
				if (maxExceptions > 0 && exceptions.size() < maxExceptions) {
					exceptions.add(e);
				}
				if (maxExceptions > 0 && exceptions.size() >= maxExceptions) {
					throw new ExceptionLimitReachedException();
				}
			}
		}
		
		private LinkedList<SegmentIntegrityException> getExceptions() {
			return exceptions;
		}
	}


	private class SegmentAuditor {
		
		private final SegmentProbe segmentProbe;

		private SegmentAuditor(SegmentProbe segmentProbe) {
			this.segmentProbe = segmentProbe;
		}
				
		private int wrapIndex(int unwrappedIndex) {
			return unwrappedIndex & segmentProbe.getIndexMask();
		}
		
		private int bucketIndex(long hashCode) {
			return (int) ((hashCode >>> segmentProbe.getLocalDepth()) & segmentProbe.getIndexMask());
		}
		
		boolean isBucketEmpty(int bucketIndex) {
			return segmentProbe.getBuckets().get(bucketIndex) == NULL_OFFSET;
		}
				
		@SuppressWarnings("rawtypes")
		private boolean bucketContainsEntry(int bucketIndex, LargeHashMap.Entry entry) {
			int offset = segmentProbe.getBuckets().get(bucketIndex);
			while (offset != NULL_OFFSET) {
				LargeHashMap.Entry bucketEntry = (LargeHashMap.Entry)segmentProbe.getEntries().get(wrapIndex(bucketIndex+offset));
				if (entry == bucketEntry) {
					return true;
				}
				int nextOffset = segmentProbe.getOffsets().get(wrapIndex(bucketIndex + offset));
				if (nextOffset != NULL_OFFSET) {
					if (nextOffset < 0 || nextOffset >= HOP_RANGE || nextOffset < offset) {
						return false;
					}
				}
				offset = nextOffset;
			}
			return false;
		}
		
		private LinkedList<SegmentIntegrityException> verifySegmentIntegrity(boolean throwImmediately, int maxExceptions) 
		throws ProbeInternalException, SegmentIntegrityException {	
			ExceptionAccumulator exceptions = new ExceptionAccumulator(throwImmediately, maxExceptions);
			int bucketEntryCount = 0;
			segmentProbe.getSegmentLock().lock();
			try {
				AtomicIntegerArray buckets = segmentProbe.getBuckets();
				AtomicIntegerArray offsets = segmentProbe.getOffsets();
				AtomicReferenceArray<LargeHashMap.Entry<?,?>> entries = (AtomicReferenceArray<LargeHashMap.Entry<?,?>>)segmentProbe.getEntries();
				nextBucket:
				for (int bucketIndex = 0; bucketIndex < buckets.length(); bucketIndex++) {
					if (!isBucketEmpty(bucketIndex)) {
						int offset = buckets.get(bucketIndex);
						if (offset < 0 || offset > HOP_RANGE) {
							exceptions.noteException(new IllegalBucketValueException(segmentProbe, bucketIndex, offset));
							continue nextBucket;
						}
												
						while (offset != NULL_OFFSET) {
							bucketEntryCount++;
							@SuppressWarnings("rawtypes")
							LargeHashMap.Entry entry = entries.get(wrapIndex(bucketIndex + offset));
							if (entry == null) {
								exceptions.noteException(new NullEntryInBucketException(segmentProbe, bucketIndex, offset));
							} else {
								int entrySharedBits = (int)(segmentProbe.getEntryHashCode(entry) & segmentProbe.getSharedBitsMask());
								if (entrySharedBits != segmentProbe.getSharedBits()) {
									exceptions.noteException(new WrongSegmentException(segmentProbe, bucketIndex, offset, entrySharedBits));
								}
								
								int entryBucketIndex = bucketIndex(segmentProbe.getEntryHashCode(entry));
								if (entryBucketIndex != bucketIndex) {
									exceptions.noteException(new WrongBucketException(segmentProbe, bucketIndex, offset, entryBucketIndex));								
								}
							}
		
							int nextOffset = offsets.get(wrapIndex(bucketIndex + offset));
							if (nextOffset != NULL_OFFSET) {
								if (nextOffset < 0 || nextOffset >= HOP_RANGE) {
									exceptions.noteException(new IllegalOffsetValueException(segmentProbe, bucketIndex, offset, nextOffset));
									continue nextBucket;
								}
								if (nextOffset < offset) {
									exceptions.noteException(new BucketOrderException(segmentProbe, bucketIndex, offset, nextOffset));										
									continue nextBucket;
								}
							}
							offset = nextOffset;
						}
					}
				}
				int entryCount = segmentProbe.getEntryCount();
				int nonNullEntryCount = 0;			
				for (int i = 0; i < entries.length(); i++) {
					@SuppressWarnings("rawtypes")
					LargeHashMap.Entry entry = entries.get(i);
					if (entry != null) {
						int entryBucketIndex = bucketIndex(segmentProbe.getEntryHashCode(entry));
						if (!bucketContainsEntry(entryBucketIndex, entry)) {
							exceptions.noteException(new EntryNotReachableException(segmentProbe, entryBucketIndex, i));
						}
						nonNullEntryCount++;
					}
				}
				if (bucketEntryCount != entryCount || nonNullEntryCount != entryCount) {
					exceptions.noteException(new EntryCountException(segmentProbe, bucketEntryCount, entryCount, nonNullEntryCount));
				}
				return exceptions.getExceptions();
			}
			catch(ExceptionLimitReachedException e) {
				return exceptions.getExceptions();
			}
			finally {
				segmentProbe.getSegmentLock().unlock();
			}
		}
	}
	
	private final ConcurrentLargeHashMapProbe mapProbe;
	private final int NULL_OFFSET;
	private final int HOP_RANGE;

	/**
	 * @param map
	 */
	public ConcurrentLargeHashMapAuditor(ConcurrentHashMap<?,?> map) {
		mapProbe = new ConcurrentLargeHashMapProbe(map);
		NULL_OFFSET = mapProbe.getNullOffset();
		HOP_RANGE = mapProbe.getHopRange();
	}
	
	
	/** Performs an exhaustive examination of the internal data structures of 
	 * an instance of {@code ConcurrentHashMap}, to verify the map's
	 * integrity. If the value of {@code throwImmediately} is {@code true},
	 * this method will throw an exception upon encountering an error.
	 * Otherwise ({@code throwImmediately} is {@code false}) this method will
	 * return a list of exceptions that describe errors encountered
	 * during the examination. If the value {@code maxExceptions} is greater
	 * than zero, the examination will return at most {@code maxExceptions}
	 * exceptions in the list. Otherwise ({@code maxExceptions <= 0}) this
	 * method will return exceptions for all errors encountered in the map
	 * inspection. If {@code throwImmediately} is {@code true}, the value
	 * of {@code maxExceptions} is ignored.<p>
	 * Each segment in the map is locked during the segment's examination.
	 * In general, it is best to perform this operation when the 
	 * map is quiescent. It can be used, however, concurrently with
	 * operations on the map. Update operations on a segment will be blocked
	 * during the segment's examination. Retrieval operations will not be
	 * affected. 
	 * @param throwImmediately if {@code true}, the first error encountered 
	 * will cause an exception to be thrown; otherwise, exceptions will be 
	 * accumulated and returned in a list
	 * @param maxExceptions if {@code > 0}, limit on the number of exceptions 
	 * returned in the resulting list; otherwise, no limit is imposed;
	 * ignored if {@code throwImmediately} is {@code true}
	 * @return list of exceptions describing encountered errors, if {@code
	 * throwImmediately} is {@code false}
	 * @throws BucketOrderException
	 * @throws EntryCountException
	 * @throws EntryNotReachableException
	 * @throws IllegalBucketValueException
	 * @throws IllegalOffsetValueException
	 * @throws NullEntryInBucketException
	 * @throws WrongBucketException
	 * @throws WrongSegmentException
	 * @throws ConcurrentLargeHashMapProbe
	 */
	@SuppressWarnings("javadoc")
	public LinkedList<SegmentIntegrityException> verifyMapIntegrity(boolean throwImmediately, int maxExceptions) throws SegmentIntegrityException {
		LinkedList<SegmentIntegrityException> exceptions = new LinkedList<SegmentIntegrityException>();
		Iterator<SegmentProbe> segIter = mapProbe.getSegmentIterator();
		while (segIter.hasNext()) {
			SegmentProbe segProbe = segIter.next();
			SegmentAuditor segAuditor = new SegmentAuditor(segProbe);
			int currentExceptions = exceptions.size();
			int segmentExceptionLimit;
			if (maxExceptions > 0) {
				segmentExceptionLimit = maxExceptions - currentExceptions;
			} else {
				segmentExceptionLimit = 0;
			}
			exceptions.addAll(segAuditor.verifySegmentIntegrity(throwImmediately, segmentExceptionLimit));
			if (maxExceptions > 0 && exceptions.size() >= maxExceptions) {
				return exceptions;
			}
		}
		return exceptions;
	}

}
