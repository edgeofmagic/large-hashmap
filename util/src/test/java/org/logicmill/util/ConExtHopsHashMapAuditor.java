package org.logicmill.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.logicmill.util.ConExtHopsHashMapProbe.ProbeInternalException;
import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;

/** An object that verifies the integrity of an instance of 
 * {@link ConExtHopsHashMap}. This class uses {@link ConExtHopsHashMapProbe}
 * to gain access to private members of {@code ConExtHopsHashMap}.
 * 
 * The auditor can be used on the map in its entirety:<pre><code>
 * ConExtHopsHashMap<String, Integer> map = new ConExtHopsHashMap( ... );
 * ConExtHopsHashMapAuditor auditor = new ConExtHopsHashMapAuditor(map);
 * ...
 * auditor.verifyMapIntegrigy(true, 0);
 * </code></pre>
 * or on individual segments:
 * 
 * @author David Curtis
 *
 */
public class ConExtHopsHashMapAuditor {
	
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


	public class SegmentAuditor {
		
		private final SegmentProbe segmentProbe;
		private final ConExtHopsHashMapProbe mapProbe;

		public SegmentAuditor(SegmentProbe segmentProbe) {
			this.segmentProbe = segmentProbe;
			mapProbe = segmentProbe.getMapProbe();
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
		
		private long getBucketMap(int bucketIndex) {
			assert bucketIndex >= 0 && bucketIndex < segmentProbe.getBuckets().length();
			long bucketMap = 0;
			int oldTimeStamp, newTimeStamp = 0;
			retry:
			do {
				bucketMap = 0;
				oldTimeStamp = segmentProbe.getTimeStamps().get(bucketIndex);
				int nextOffset = segmentProbe.getBuckets().get(bucketIndex);
				if (nextOffset == NULL_OFFSET) {
					return 0L;
				}
				while (nextOffset != NULL_OFFSET) {
					int nextIndex = wrapIndex(bucketIndex + nextOffset);
					@SuppressWarnings("rawtypes")
					LargeHashMap.Entry entry = (LargeHashMap.Entry)segmentProbe.getEntries().get(nextIndex);
					if (entry == null) {
						/*
						 * Concurrent update; try again.
						 */
						continue retry;
					}
					if (bucketIndex(segmentProbe.getEntryHashCode(entry)) != bucketIndex) {
						/*
						 * Concurrent update; try again.
						 */
						continue retry;
					} else {
						bucketMap |= 1 << nextOffset;
					}
					nextOffset = segmentProbe.getOffsets().get(nextIndex);
				}
				newTimeStamp = segmentProbe.getTimeStamps().get(bucketIndex);
			} while (newTimeStamp != oldTimeStamp);
			return bucketMap;
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
		
		/**
		 * @param throwImmediately
		 * @param maxExceptions
		 * @return
		 * @throws ProbeInternalException
		 * @throws SegmentIntegrityException
		 */
		public LinkedList<SegmentIntegrityException> verifySegmentIntegrity(boolean throwImmediately, int maxExceptions) 
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
	
	private final ConExtHopsHashMap map;
	private final ConExtHopsHashMapProbe mapProbe;
	private final int NULL_OFFSET;
	private final int HOP_RANGE;
	private final boolean GATHER_METRICS;

	public ConExtHopsHashMapAuditor(ConExtHopsHashMap map) {
		this.map = map;
		mapProbe = new ConExtHopsHashMapProbe(map);
		NULL_OFFSET = mapProbe.getNullOffset();
		HOP_RANGE = mapProbe.getHopRange();
		GATHER_METRICS = mapProbe.getGatherMetrics();
	}
	
	public LinkedList<SegmentIntegrityException> verifyMapIntegrity(boolean throwImmediately, int maxExceptions) throws SegmentIntegrityException {
		LinkedList<SegmentIntegrityException> exceptions = new LinkedList<SegmentIntegrityException>();
		Iterator<SegmentProbe> segIter = mapProbe.getSegmentIterator();
		while (segIter.hasNext()) {
			SegmentProbe segProbe = segIter.next();
			SegmentAuditor segAuditor = new SegmentAuditor(segProbe);
			exceptions.addAll(segAuditor.verifySegmentIntegrity(throwImmediately, maxExceptions));
			if (exceptions.size() >= maxExceptions) {
				return exceptions;
			}
		}
		return exceptions;
	}

}
