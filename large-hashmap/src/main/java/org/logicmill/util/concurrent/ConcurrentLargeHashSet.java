package org.logicmill.util.concurrent;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

//import org.logicmill.util.LargeHashCore;
import org.logicmill.util.LargeHashSet;
import org.logicmill.util.LongHashable;

/**
 * @author David Curtis
 *
 * @param <E>
 */
public class ConcurrentLargeHashSet<E> implements LargeHashSet<E> {

	
	private static class DefaultEntryAdapter<E> implements LargeHashSet.EntryAdapter<E> {

		@Override
		public long getLongHashCode(Object entryKey) {
			if (entryKey instanceof LongHashable) {
				return ((LongHashable)entryKey).getLongHashCode();
			} else {
				throw new IllegalArgumentException("key must implement org.logicmill.util.LongHashable");
			}
		}

		@Override
		public boolean keyMatches(E mappedEntry, Object entryKey) {
			return mappedEntry.equals(entryKey);
		}

	}
	
	abstract static class RemoveHandler<E> {
		abstract boolean remove(E mappedEntry);
	}
	
	abstract static class ReplaceHandler<E> {
		abstract E replaceWith(E mappedEntry);
	}

	
	/*
	 * REGARDING SEGMENT STRUCTURE
	 * 
	 * The essential information about a segment's structure is expressed in
	 * three arrays: buckets, entries, offsets. The arrays are all of length 
	 * segmentSize.
	 * 
	 * A bucket is linked list of entries whose hash codes collide at the same
	 * index in the buckets array. Linked lists are expressed as a sequence of
	 * offsets from the bucket index.
	 * 
	 * INDICES AND OFFSETS
	 * 
	 * A hash code value is converted to a array index for the buckets array 
	 * like this:
	 * 
	 * 		bucketIndex = (hashCode >>> localDepth) & indexMask;  
	 * 
	 * where the (& indexMask) is equivalent to (% segmentSize). An offset is
	 * applied to an index like this:
	 * 
	 * 		nextIndex = (bucketIndex + offset) & indexMask;
	 * 
	 * The arrays are effectively circular -- indices that would exceed the 
	 * array size wrap around to the beginning of the array.
	 * 
	 * buckets[bucketIndex] contains the offset (from bucketIndex) to the first
	 * entry in the bucket (or NULL_OFFSET if the bucket is empty):
	 * 
	 * 		if (buckets[bucketIndex] != NULL_OFFSET) {
	 * 			int index = (bucketIndex + buckets[bucketIndex]) & indexMask;
	 * 			Entry firstEntry = entries[index];
	 * 			...
	 * 
	 * Subsequent entries in the bucket by following the links in offsets:
	 * 
	 * 		while (offsets[index] != NULL_OFFSET) {
	 * 			nextIndex = (bucketIndex + offsets[index]) & indexMask;
	 * 			Entry nextEntry = entries[nextIndex];
	 * 			...
	 * 			index = nextIndex;
	 * 		}
	 * 
	 * OTHER IMPORTANT SEGMENT STUFF:
	 * 
	 * The timeStamps array is used to implement non-blocking concurrent 
	 * retrieval. When a get() operation traverses the bucket at 
	 * buckets[bucketIndex], it compares the value of timeStamps[bucketIndex] 
	 * before and after the traversal. If the time stamps don't match, the 
	 * bucket was modified during the traversal, so the traversal must be
	 * repeated.
	 */
	private class Segment {
		
		/*
		 * For annotation only, not operationally significant. Accessed by
		 * ConcurrentLargeHashMapProbe.SegmentProbe, with reflection, so there
		 * is no local use of this private field.
		 */
		@SuppressWarnings("unused")
		private final int serialID;
		
		/*
		 * A segment is uniquely identified by localDepth and sharedBits. 
		 * localDepth is the number of low-order bits in the hash code whose 
		 * contents are fixed for this segment (that is, all entry hash codes
		 * in the segment have the same value in those bits; sharedBits is 
		 * that value. See discussions of Extendible Hashing for their full 
		 * significance.
		 */
		private final int localDepth;
		private final int sharedBits;	
		private final long sharedBitsMask;
		private final int indexMask;
		
		/*
		 * Segment arrays, discussed in detail above. Note: these
		 * are atomic only because Java doesn't support arrays of 
		 * volatile types. Atomicity isn't needed, (except in the 
		 * case of timeStamps) since modifications are always protected 
		 * by segment locks, but guaranteeing order of visibility is 
		 * critical.
		 */
		private final AtomicReferenceArray<E> entries;
		private final AtomicIntegerArray offsets;
		private final AtomicIntegerArray buckets;
		private final AtomicIntegerArray timeStamps;

		private volatile int entryCount;
		
		/*
		 * Prevents an updating thread from modifying a segment
		 * that has already been removed from the directory.
		 */
		private boolean invalid;
		
		private final ReentrantLock lock;
		
		
		/*
		 * Accumulated concurrency event metrics, for reporting by
		 * ConcurrentLargeHashMapInspector (the which see for a detailed 
		 * discussion of these values).
		 */	
		private final AtomicLong readRetrys;
		private final AtomicInteger maxReadRetrys;
		private final AtomicLong bucketRetrys;
		private final AtomicInteger maxBucketRetrys;
		private volatile long recycleCount;
		private volatile long accumRecycleDepth;
		private volatile int maxRecycleDepth;
				
		/*
		 * When a segment is split, metrics are copied to one of the        
		 * segments to prevent loss.
		 */
		private void copyMetrics(Segment from) {
			this.recycleCount = from.recycleCount;
			this.readRetrys.set(from.readRetrys.get());
			this.maxReadRetrys.set(from.maxReadRetrys.get());
			this.bucketRetrys.set(from.bucketRetrys.get());
			this.maxBucketRetrys.set(from.maxBucketRetrys.get());
			this.accumRecycleDepth = from.accumRecycleDepth;
			this.maxRecycleDepth = from.maxRecycleDepth;
		}
				
		private Segment(int localDepth, int sharedBits) {
			serialID = segmentSerialID.getAndIncrement();
			this.localDepth = localDepth;
			this.sharedBits = sharedBits;
			sharedBitsMask = (long)((1 << localDepth) - 1);
			indexMask = segmentSize - 1;
			offsets = new AtomicIntegerArray(segmentSize);
			buckets = new AtomicIntegerArray(segmentSize);
			entries = new AtomicReferenceArray<E>(segmentSize);
			timeStamps = new AtomicIntegerArray(segmentSize);
			for (int i = 0; i < segmentSize; i++) {
				buckets.set(i,NULL_OFFSET);
			}
			lock = new ReentrantLock(true);
			entryCount = 0;
			invalid = false;
	
			recycleCount = 0L;
			readRetrys = new AtomicLong(0L);
			maxReadRetrys = new AtomicInteger(0);
			bucketRetrys = new AtomicLong(0L);
			maxBucketRetrys = new AtomicInteger(0);
			accumRecycleDepth = 0L;
			maxRecycleDepth = 0;
			
		}
			
		private boolean sharedBitsMatchSegment(long hashCode) {
			return sharedBits(hashCode) == sharedBits;	
		}
		
		/* Splits a segment when it reaches the load threshold. Splitting 
		 * increases localDepth by one, so the new segments have sharedBits 
		 * values of (binary) 1??? and 0??? (where ??? is the sharedBits 
		 * value of the original segment). 
		 */
		@SuppressWarnings("unchecked")
		private Segment[] split() {
			Segment[] splitPair = new ConcurrentLargeHashSet.Segment[2];
			int  newLocalDepth = localDepth + 1;
			int newSharedBitMask = 1 << localDepth;
			splitPair[0] = new Segment(newLocalDepth, sharedBits);
			splitPair[1] = new Segment(newLocalDepth, sharedBits | newSharedBitMask);
			for (int i = 0; i < segmentSize; i++) {
				E entry = entries.get(i);
				if (entry != null) {
					if ((entryAdapter.getLongHashCode(entry) & (long)newSharedBitMask) == 0) {
						splitPair[0].splitPut(entry);
					} else {
						splitPair[1].splitPut(entry);
					}
				}
			}
			return splitPair;
		}
		
		/*
		 * Calculates a bucket index from a hash code value.
		 */
		private int bucketIndex(long hashCode) {
			return (int)((hashCode >>> localDepth) & (long)indexMask);
		}
		
		private int sharedBits(long hashCode) {
			return (int)(hashCode & sharedBitsMask);
		}
		
		private int wrapIndex(int unwrappedIndex) {
			return unwrappedIndex & indexMask;
		}
		
		/*
		 * splitPut(Entry<K,V>) is only used during a split, which allows 
		 * some simplifying assumptions to be made, to wit:
		 * 	1) overflow shouldn't happen,
		 * 	2) the target segment is visible only to the current thread, and
		 * 	2) the key/entry can't already be in the segment.
		 */
		private void splitPut(E entry) {
			// assert sharedBits(entry.getHashCode()) == sharedBits;
			try {
				placeWithinHopRange(bucketIndex(entryAdapter.getLongHashCode(entry)), entry);
			} catch (SegmentOverflowException soex) {
				/*
				 *  This should not occur during split operation. Freak out.
				 */
				throw new IllegalStateException("overflow during split");
			}
			entryCount++;			
		}
		
		/*
		 * Puts the entry in the map if a suitable slot can be located. 
		 * Otherwise, throws SegmentOverflowException. This method and 
		 * findCloserSlot() constitute the core of the Hopscotch hashing 
		 * algorithm implementation.
		 */
		private void placeWithinHopRange(int bucketIndex, E entry) 
				throws SegmentOverflowException {
			int freeSlotOffset = 0;
			int freeSlotIndex = bucketIndex;
			/*
			 * Finds a free slot (where entries[] is null) that is within
			 * ADD_RANGE offset from the bucket index.
			 */
			while (entries.get(freeSlotIndex) != null && freeSlotOffset < ADD_RANGE) {
				freeSlotOffset++;
				freeSlotIndex = wrapIndex(bucketIndex + freeSlotOffset);
			}
			if (freeSlotOffset >= ADD_RANGE) {
				/*
				 * No available slot; calling thread will catch and split.
				 */
				throw new SegmentOverflowException();
			}
			// assert entries.get(freeSlotIndex) == null;

			/*
			 * If the available slot isn't within HOP_RANGE, so do the 
			 * Hopscotch thing -- swap the free slot with an occupied slot 
			 * closer to the target bucket. Repeat until the free slot is 
			 * within HOP_RANGE of the bucket.
			 */
			while (freeSlotOffset >= HOP_RANGE) {
				freeSlotIndex = findCloserSlot(freeSlotIndex);
				if (freeSlotIndex == NULL_INDEX) {
					/*
					 * No available slot in HOP_RANGE; calling thread will 
					 * catch and force split
					 */
					throw new SegmentOverflowException();
				}
				freeSlotOffset = (freeSlotIndex - bucketIndex) & indexMask;
			}
			// assert freeSlotOffset < HOP_RANGE;	
			// assert wrapIndex(bucketIndex + freeSlotOffset) == freeSlotIndex;
			entries.set(freeSlotIndex, entry);
			insertEntryIntoBucket(freeSlotOffset, bucketIndex);
		}
		
		/*
		 * Tries to move the free slot downward (in direction of decreasing 
		 * offset) by exchanging it with an entry in another bucket, (very) 
		 * roughly analogous to an electron hole migrating through a conductor.
		 * The distance moved be at most HOP_RANGE - 1. Returns either the 
		 * index of the new free slot or NULL_INDEX if none is available.
		 */
		private int findCloserSlot(final int freeSlotIndex) {
			/* 
			 * Start with HOP_RANGE-1 and moving up, try to find a bucket
			 * containing an entry that can be swapped with the free slot.
			 */		
			for (int freeSlotOffset = (HOP_RANGE - 1); freeSlotOffset > 0; freeSlotOffset--) {
				int swapBucketIndex = wrapIndex(freeSlotIndex - freeSlotOffset);	
				/*
				 *  If the first entry in the bucket exists and is usable (its 
				 *  index is "less than" [modulo array size] the index of the 
				 *  free slot), then move its contents to free slot.
				 */
				// assert freeSlotIndex == wrapIndex(swapBucketIndex + freeSlotOffset);
				final int oSwapSlot = buckets.get(swapBucketIndex);
				if (oSwapSlot != NULL_OFFSET && oSwapSlot < freeSlotOffset) {
					final int swapSlotIndex = wrapIndex(swapBucketIndex + oSwapSlot);
					entries.set(freeSlotIndex, entries.get(swapSlotIndex));
					// assert freeSlotOffset == wrapIndex(freeSlotIndex + swapBucketIndex);
					/*
					 * Fix the list offsets. offsets[swapSlotIndx] contains 
					 * either NULL_OFFSET or the offset to the rest of the 
					 * bucket's contents. 
					 */
					final int restOfBucketOffset = offsets.get(swapSlotIndex);
					if (restOfBucketOffset == NULL_OFFSET || freeSlotOffset < restOfBucketOffset) {
						/*
						 * The destination of the move is the first (and 
						 * possibly only) entry in the bucket, so change the
						 * offset in buckets.
						 */
						offsets.set(freeSlotIndex, restOfBucketOffset);
						/*
						 * Serialization point:
						 */
						buckets.set(swapBucketIndex, freeSlotOffset);
						timeStamps.incrementAndGet(swapBucketIndex);
						entries.set(swapSlotIndex, null);
						offsets.set(swapSlotIndex, NULL_OFFSET);
						return swapSlotIndex;			
					} else {
						/*
						 * At least one entry in the bucket precedes the
						 * destination of the move. Find (prevOffset, nextOffset) 
						 * such that prevOffset < freeSlotOffset < nextOffset, 
						 * and adjust offsets to insert swapped entry into its
						 * new location in the bucket.
						 */
						int prevOffset = restOfBucketOffset;
						// assert prevOffset != NULL_OFFSET && prevOffset < freeSlotOffset;
						int prevIndex = wrapIndex(swapBucketIndex + prevOffset);
						int nextOffset = offsets.get(prevIndex);
						while (nextOffset != NULL_OFFSET && nextOffset < freeSlotOffset) {
							prevIndex = wrapIndex(swapBucketIndex + nextOffset);
							prevOffset = nextOffset;
							nextOffset = offsets.get(prevIndex);
						}
						// assert prevOffset < freeSlotIndex && (freeSlotOffset < nextOffset || nextOffset == NULL_OFFSET);
						
						offsets.set(freeSlotIndex, nextOffset);
						/*
						 * Serialization point:
						 */
						offsets.set(prevIndex, freeSlotOffset);
						/*
						 * At this point, the moved entry is in the list twice.
						 * That is OK, as long as it is never invisible.
						 */
						buckets.set(swapBucketIndex, restOfBucketOffset);
						timeStamps.incrementAndGet(swapBucketIndex);
						entries.set(swapSlotIndex, null);
						offsets.set(swapSlotIndex, NULL_OFFSET);
						return swapSlotIndex;
					}
				}
			}
			return NULL_INDEX;
		}
		
		/*
		 * Upon invocation, entries[(iBucket+oNode) & indexMask] contains
		 * the entry to be inserted into bucket at iBucket. Insert that
		 * entry into the bucket by adjusting the appropriate offsets.
		 * 
		 * Note: time stamps are not modified, since a concurrent retrieval
		 * can't possibly fail to find an entry that was in the bucket before
		 * the onset of the retrieval. That can only happen when the retrieval
		 * is concurrent with a remove, recycle, or free slot migration in 
		 * findCloserSlot
		 */
		private void insertEntryIntoBucket(int entryOffset, int bucketIndex) {
			int entryIndex = wrapIndex(bucketIndex + entryOffset);
			int firstOffset = buckets.get(bucketIndex);
			if (firstOffset == NULL_OFFSET || firstOffset > entryOffset) {
				/*
				 * Either the bucket was empty, or the offset of the new entry
				 * is smaller than the first offset in the bucket. Insert the 
				 * new entry at the beginning of the bucket list.
				 */
				offsets.set(entryIndex, firstOffset);	
				/*
				 * Serialization point:
				 */
				buckets.set(bucketIndex, entryOffset);
			} else {
				/*
				 * At least one entry in the bucket precedes the new entry.
				 * Find oPrev, oNext such that oPrev < oNode < oNext, 
				 * and adjust offsets to insert the entry into the bucket.
				 */
				int prevOffset = firstOffset;
				int prevIndex = wrapIndex(bucketIndex + prevOffset);
				int nextOffset = offsets.get(prevIndex);
				while (nextOffset != NULL_OFFSET && nextOffset < entryOffset) {
					prevIndex = wrapIndex(bucketIndex + nextOffset);
					prevOffset = nextOffset;
					nextOffset = offsets.get(prevIndex);
				}
				offsets.set(entryIndex, nextOffset);
				/*
				 * Serialization point:
				 */
				offsets.set(prevIndex, entryOffset);
			}				
		
		}
		
		private E replace(Object entryKey, long hashCode, ReplaceHandler<E> handler) {
			int bucketIndex = bucketIndex(hashCode);
			/*
			 * Search for entry with key
			 */
			int nextOffset = buckets.get(bucketIndex);
			while (nextOffset != NULL_OFFSET) {
				int entryIndex = wrapIndex(bucketIndex + nextOffset);
				E mappedEntry = entries.get(entryIndex);
				if (entryAdapter.getLongHashCode(mappedEntry) == hashCode && entryAdapter.keyMatches(mappedEntry,entryKey)) {		
					E newEntry = handler.replaceWith(mappedEntry);
					if (newEntry == null) {
						return null;
					} else {
						entries.set(entryIndex, newEntry);
					}
					return mappedEntry;
				}
				nextOffset = offsets.get(entryIndex);
			}
			/*
			 *  Not found
			 */
			return null;	
		}
	
		
		/*
		 * Implements replace(Object key, V value) and replace (Object key, V oldValue, V newValue).
		 * If oldValue is null, treat it as replace(key, value), otherwise, only replace
		 * if existing entry value equals oldValue.
		 */
		private E replace(Object entryKey, long hashCode, E newEntry) {
			int bucketIndex = bucketIndex(hashCode);
			/*
			 * Search for entry with key
			 */
			int nextOffset = buckets.get(bucketIndex);
			while (nextOffset != NULL_OFFSET) {
				int entryIndex = wrapIndex(bucketIndex + nextOffset);
				E mappedEntry = entries.get(entryIndex);
				if (entryAdapter.getLongHashCode(mappedEntry) == hashCode && entryAdapter.keyMatches(mappedEntry,entryKey)) {
					entries.set(entryIndex, newEntry);
					return mappedEntry;						
				}
				nextOffset = offsets.get(entryIndex);
			}
			/*
			 *  Not found
			 */
			return null;	
		}
			
		/*
		 * Implements put() and putIfAbsent() on the appropriate segment. The
		 * segment has already been locked by the thread on entry.
		 */
		private E put(E newEntry, long hashCode, boolean replaceIfPresent) 
		throws SegmentOverflowException {
			// assert sharedBits(hashValue) == sharedBits;
			int bucketIndex = bucketIndex(hashCode);
			/*
			 * Search for entry with key
			 */
			int nextOffset = buckets.get(bucketIndex);
			while (nextOffset != NULL_OFFSET) {
				int entryIndex = wrapIndex(bucketIndex + nextOffset);
				E mappedEntry = entries.get(entryIndex);
				if (entryAdapter.getLongHashCode(mappedEntry) == hashCode  && entryAdapter.keyMatches(mappedEntry, newEntry)) {
					if (replaceIfPresent) {
						entries.set(entryIndex, newEntry);
					}
					return mappedEntry;						
				}
				nextOffset = offsets.get(entryIndex);
			}
			/*
			 *  Not found, insert the new entry.
			 */
			placeWithinHopRange(bucketIndex, newEntry);
			entryCount++;
			mapEntryCount.incrementAndGet();
			return null;
		}
	
		
		private E get(Object entryKey, long hashCode) {
			/*
			 * In very high update-rate environments, it might be
			 * necessary to limit re-tries, and go to a linear search of
			 * all offsets in {0 .. HOP_RANGE-1} if the limit is exceeded.
			 */
			int localRetrys = 0;
			// assert sharedBits(hashValue) == sharedBits;
			int bucketIndex = bucketIndex(hashCode);
			int oldTimeStamp, newTimeStamp = 0;
			try {
				retry:
				do {
					oldTimeStamp = timeStamps.get(bucketIndex);
					int nextOffset = buckets.get(bucketIndex);
					while (nextOffset != NULL_OFFSET) {
						int nextIndex = wrapIndex(bucketIndex + nextOffset);
						E mappedEntry = entries.get(nextIndex);
						long entryHashCode = entryAdapter.getLongHashCode(mappedEntry);
						if (mappedEntry == null) {
							/*
							 * Concurrent update, re-try.
							 */
							localRetrys++;
							continue retry;
						}
						if (bucketIndex(entryHashCode) != bucketIndex) {
							/*
							 * Concurrent update, re-try.
							 */
							localRetrys++;
							continue retry;
						}
						if (entryHashCode == hashCode && entryAdapter.keyMatches(mappedEntry, entryKey)) {
							return mappedEntry;
						}
						nextOffset = offsets.get(nextIndex);
					}
					/*
					 * The key wasn't found. Re-try if the time stamps
					 * didn't match.
					 */
					newTimeStamp = timeStamps.get(bucketIndex);
					if (newTimeStamp != oldTimeStamp) {
						localRetrys++;
						continue retry;
					} else {
						return null;
					}
				} while (true);
			} 
			/*
			 * Register the re-try attempts with the accumulated metrics.
			 */
			finally {
				if (GATHER_EVENT_DATA) {
					if (localRetrys > 0) {
						int maxRetrys = maxReadRetrys.get();
						while (localRetrys > maxRetrys) {
							if (maxReadRetrys.compareAndSet(maxRetrys, localRetrys)) {
								break;
							}
							maxRetrys = maxReadRetrys.get();
						}
						readRetrys.addAndGet(localRetrys);
					}
				}
			}
		}
		
		/*
		 * Tries to optimize list structure by decreasing the average distance 
		 * between entries in a bucket. recycle() is called from remove();
		 * freeSlotIndex is the index of the entry that was removed.
		 * 
		 * At present, the approach is very simple: examine the entry at 
		 * freeSlotIndex - 1; if non-null and not the last entry in its 
		 * bucket, move the next entry in that bucket to the free
		 * slot. Recurse on the slot just vacated.
		 * 
		 * Returns the depth of recursion (count of successful recycles).
		 */
		private int recycle(int freeSlotIndex) {
			int adjacentEntryIndex = (freeSlotIndex - 1) & indexMask;
			E adjacentEntry = entries.get(adjacentEntryIndex);
			if (adjacentEntry != null) {
				int adjacentEntryNextOffset = offsets.get(adjacentEntryIndex);
				if (adjacentEntryNextOffset != NULL_OFFSET) {
					int contractingBucketIndex = bucketIndex(entryAdapter.getLongHashCode(adjacentEntry));
					int movingEntryIndex = 
							wrapIndex(contractingBucketIndex + adjacentEntryNextOffset);
					entries.set(freeSlotIndex, entries.get(movingEntryIndex));
					offsets.set(freeSlotIndex, offsets.get(movingEntryIndex));
					int freeSlotOffset = ((freeSlotIndex - contractingBucketIndex) & indexMask);
					/*
					 * Serialization point:
					 */
					offsets.set(adjacentEntryIndex, freeSlotOffset);
					timeStamps.incrementAndGet(contractingBucketIndex);
					entries.set(movingEntryIndex, null);
					offsets.set(movingEntryIndex, NULL_OFFSET);
					return 1 + recycle(movingEntryIndex);
				}
			}
			return 0;
		}

		
		
		/*
		 * Public remove() delegates to this method on the appropriate segment.
		 * The segment has already been locked by the calling thread. 
		 */
		private E remove(Object entryKey, long hashCode, RemoveHandler<E> handler) {
			int bucketIndex = bucketIndex(hashCode);			
			int nextOffset = buckets.get(bucketIndex);
			if (nextOffset == NULL_OFFSET) {
				return null;
			}
			/*
			 * If the key is first in the bucket, it's a special case
			 * since the buckets array is updated.
			 */
			int nextIndex = wrapIndex(bucketIndex + nextOffset);
			E mappedEntry = entries.get(nextIndex);
			// assert entry != null;
			if (entryAdapter.getLongHashCode(mappedEntry) == hashCode && entryAdapter.keyMatches(mappedEntry, entryKey)) {
				/*
				 * First entry in the bucket was the key to be removed.
				 */
				if (handler != null && !handler.remove(mappedEntry)) {
					return null;
				}
				/*
				 * Serialization point:
				 */
				buckets.set(bucketIndex, offsets.get(nextIndex));
				timeStamps.incrementAndGet(bucketIndex);
				entries.set(nextIndex, null);
				offsets.set(nextIndex, NULL_OFFSET);
				entryCount--;
				mapEntryCount.decrementAndGet();
				int recycleDepth = recycle(nextIndex);
				if (GATHER_EVENT_DATA) {
					if (recycleDepth > 0) {
						recycleCount++;
						accumRecycleDepth += recycleDepth;
						if (recycleDepth > maxRecycleDepth) {
							maxRecycleDepth = recycleDepth;
						}
					}
					
				}
				return mappedEntry;
			} 
			
			/*
			 * The first entry in the bucket is not the key being removed. 
			 * Traverse the list, looking for the key.
			 */
			nextOffset = offsets.get(nextIndex);
			while (nextOffset != NULL_OFFSET) {
				int prevIndex = nextIndex;
				nextIndex = wrapIndex(bucketIndex + nextOffset);
				nextOffset = offsets.get(nextIndex);
				mappedEntry = entries.get(nextIndex);
				if (entryAdapter.getLongHashCode(mappedEntry) == hashCode && entryAdapter.keyMatches(mappedEntry, entryKey)) {					
					if (handler != null && !handler.remove(mappedEntry)) {
						return null;
					}

					/*
					 * Serialization point:
					 */
					offsets.set(prevIndex, offsets.get(nextIndex));
					timeStamps.incrementAndGet(bucketIndex);
					entries.set(nextIndex, null);
					offsets.set(nextIndex, NULL_OFFSET);
					entryCount--;
					mapEntryCount.decrementAndGet();
					recycle(nextIndex);
					return mappedEntry;
				}
			}
			/*
			 * Key wasn't found.
			 */
			return null;
		}
		
		/*
		 * Invoked by iteratorInternals; builds a list containing all of the 
		 * entries in the bucket. Returns null if the bucket is empty. The 
		 * calling environment allocates bucketContents array, re-using to 
		 * reduce garbage generation.
		 * 
		 * Returns the number of entries in the specified bucket.
		 */
		private int getBucket(int bucketIndex, E[] bucketContents) {
			int localRetrys = 0;
			/*
			 * In very high update-rate environments, it might be
			 * necessary to limit re-tries, and go to a linear search of
			 * all offsets in {0 .. HOP_RANGE-1} if the limit is exceeded.
			 */
			// assert bucketIndex >= 0 && bucketIndex < segmentSize;
			if (buckets.get(bucketIndex) == NULL_OFFSET) {
				return 0;
			}
			int oldTimeStamp, newTimeStamp = 0;
			int bucketSize;	
			try {
				retry:
				do {
					/*
					 * Remove any contents from previous aborted attempts.
					 */ 
					Arrays.fill(bucketContents, null);
					bucketSize = 0;
					oldTimeStamp = timeStamps.get(bucketIndex);
					int nextOffset = buckets.get(bucketIndex);
					if (nextOffset == NULL_OFFSET) {
						/*
						 * Concurrent update resulted in empty bucket.
						 */
						return 0;
					}
					while (nextOffset != NULL_OFFSET) {
						int nextIndex = wrapIndex(bucketIndex + nextOffset);
						E entry = entries.get(nextIndex);
						if (entry == null) {
							/*
							 * Concurrent update, try again.
							 */
							localRetrys++;
							continue retry;
						}
						if (bucketIndex(entryAdapter.getLongHashCode(entry)) != bucketIndex) {
							/*
							 * Concurrent update, try again.
							 */
							localRetrys++;
							continue retry;
						} else {
							bucketContents[bucketSize++] = entry;
						}
						nextOffset = offsets.get(nextIndex);
					}
					newTimeStamp = timeStamps.get(bucketIndex);
					if (newTimeStamp != oldTimeStamp) {
						/*
						 * Concurrent update, try again.
						 */
						localRetrys++;
						continue retry;
					} else {
						break;
					}
				} while (true);
				return bucketSize;
			}
			finally {
				if (GATHER_EVENT_DATA) {
					/*
					 * Register re-try attempts with accumulated metrics.
					 */
					if (localRetrys > 0) {
						int maxRetrys = maxBucketRetrys.get();
						while (localRetrys > maxRetrys) {
							if (maxBucketRetrys.compareAndSet(maxRetrys,localRetrys)) {
								break;
							}
							maxRetrys = maxBucketRetrys.get();
						}
						bucketRetrys.addAndGet(localRetrys);
					}
				}
			}
		}	
	}
	
	/*
	 * Constants.
	 */
	
	/*
	 * Compile-time flag enables data collection.
	 */
	static final boolean GATHER_EVENT_DATA = true;
	
	/*
	 * HOP_RANGE is maximum offset from the bucket index to
	 * the location of any entry that belongs to the bucket.
	 */
	private static final int HOP_RANGE = 32;
	/*
	 * ADD_RANGE is the maximum offset from the bucket
	 * index that is considered when searching
	 * for an open slot during insertion.
	 */
	static final int ADD_RANGE = 512;
	/*
	 * Null values for buckets and hops array indices in Segment,
	 * since zero is a valid offset and index.
	 */
	static final int NULL_OFFSET = -1;
	static final int NULL_INDEX = -1;

	/*
	 * Minimum and maximum configuration values are for basic sanity checks
	 * during construction.
	 */
	private static final int MIN_SEGMENT_SIZE = 4096;
	private static final int MIN_INITIAL_SEGMENT_COUNT = 2;
	private static final float MIN_LOAD_THRESHOLD = 0.1f;
	private static final float MAX_LOAD_THRESHOLD = 1.0f;
	private static final int MAX_SEGMENT_SIZE = 1 << 30;
	private static final int MAX_DIR_SIZE = 1 << 30;
			
	private final int segmentSize;
	private final ReentrantLock dirLock;
	private final AtomicReference<AtomicReferenceArray<Segment>> directory;
	/*
	 * Maximum number of entries allowed in a segment before splitting.
	 */
	private final int loadThresholdLimit;

	private volatile int segmentCount;
	private final AtomicInteger forcedSplitCount;
	private final AtomicInteger thresholdSplitCount;
	private final AtomicInteger segmentSerialID;
	
	private final AtomicLong mapEntryCount;
		
	private static class SegmentOverflowException extends Exception {
		private static final long serialVersionUID = -5917984727339916861L;	
	}	
	
	private static int nextPowerOfTwo(int num) {
		if (num > 1 << 30) {
			return 1 << 30;
		}
		int i = 1;
		while (i < num) {
			i <<= 1;
		}
		return i;
	}

	private final LargeHashSet.EntryAdapter<E> entryAdapter;
	
	/**
	 * @param segSize
	 * @param initSegCount
	 * @param loadThreshold
	 * @param entryAdapter
	 */
	public ConcurrentLargeHashSet(int segSize, int initSegCount, float loadThreshold, EntryAdapter<E> entryAdapter) {
					
		segSize = nextPowerOfTwo(segSize);		
		initSegCount = nextPowerOfTwo(initSegCount);
		
		if (segSize < MIN_SEGMENT_SIZE) {
			segSize = MIN_SEGMENT_SIZE;
		} else if (segSize > MAX_SEGMENT_SIZE) {
			segSize = MAX_SEGMENT_SIZE;
		}
		
		if (initSegCount < MIN_INITIAL_SEGMENT_COUNT) {
			initSegCount = MIN_INITIAL_SEGMENT_COUNT; 
		} else if (initSegCount > MAX_DIR_SIZE) {
			initSegCount = MAX_DIR_SIZE;
		}
		
		if (loadThreshold < MIN_LOAD_THRESHOLD) {
			loadThreshold = MIN_LOAD_THRESHOLD;
		} else if (loadThreshold > MAX_LOAD_THRESHOLD) {
			loadThreshold = MAX_LOAD_THRESHOLD;
		}
		
		if (entryAdapter == null) {
			throw new NullPointerException("entryAdapter must be non-null");
		}
		
		int initDirCapacity = initSegCount;
		segmentCount = initSegCount;
		forcedSplitCount = new AtomicInteger(0);
		thresholdSplitCount = new AtomicInteger(0);
		segmentSerialID = new AtomicInteger(0);
		int dirSize = initSegCount;
		segmentSize = segSize;
		loadThresholdLimit = (int)(((float)segSize) * loadThreshold);
		int dirMask = dirSize - 1;
		int depth = Long.bitCount(dirMask);
		AtomicReferenceArray<Segment> dir = 
				new AtomicReferenceArray<Segment>(initDirCapacity);
		for (int i = 0; i < initSegCount; i++) {
			dir.set(i, new Segment(depth, i));
		}
		dirLock = new ReentrantLock(true);
		directory = new AtomicReference<AtomicReferenceArray<Segment>>(dir);
		mapEntryCount = new AtomicLong(0L);
		
		this.entryAdapter = entryAdapter;
	}

	/**
	 * @param segSize
	 * @param initSegCount
	 * @param loadThreshold
	 */
	public ConcurrentLargeHashSet(int segSize, int initSegCount, float loadThreshold) {
		this(segSize, initSegCount, loadThreshold, new DefaultEntryAdapter<E>());
	}

	E replace(Object entryKey, ReplaceHandler<E> handler) {
		long hashCode = entryAdapter.getLongHashCode(entryKey);
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			long dirMask = dir.length() - 1;
			int segmentIndex = (int)(hashCode & (long)dirMask);
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return seg.replace(entryKey, hashCode, handler);	
				}
			}
			finally {
				seg.lock.unlock();
			}
		}
		
	}
	

	E replace(Object entryKey, E newEntry) {
		long hashCode = entryAdapter.getLongHashCode(entryKey);
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			long dirMask = dir.length() - 1;
			int segmentIndex = (int)(hashCode & (long)dirMask);
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return seg.replace(entryKey, hashCode, newEntry);	
				}
			}
			finally {
				seg.lock.unlock();
			}
		}
		
	}

	/*

	@Override
	public E replace(Object entryKey, E newEntry) {
		if (entryKey == null || newEntry == null) {
			throw new NullPointerException();
		}
		long hashCode = entryAdapter.getLongHashCode(entryKey);
		return replace(entryKey, hashCode, null, newEntry);
	}

	@Override
	public E replace(Object entryKey, Object oldEntry, E newEntry) {
		if (entryKey == null || oldEntry == null || newEntry == null) {
			throw new NullPointerException();
		}
		long hashCode = entryAdapter.getLongHashCode(entryKey);
		return replace(entryKey, hashCode, oldEntry, newEntry);
	}
	
	private E replace(Object entryKey, long hashCode, Object oldEntry, E newEntry) {
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			long dirMask = dir.length() - 1;
			int segmentIndex = (int)(hashCode & (long)dirMask);
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return seg.replace(entryKey, hashCode, oldEntry, newEntry);	
				}
			}
			finally {
				seg.lock.unlock();
			}
		}

	}
	*/

	@Override
	public E remove(Object entryKey) {
		return remove(entryKey, null);
	}
	

	E remove(Object entryKey, RemoveHandler<E> handler) {
		if (entryKey == null) {
			throw new NullPointerException();
		}
		long hashCode = entryAdapter.getLongHashCode(entryKey);
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			int dirSize = dir.length();
			long dirMask = dirSize - 1;
			int segmentIndex = (int)(hashCode & (long)dirMask);
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return seg.remove(entryKey, hashCode, handler);	
				}
			}
			finally {
				seg.lock.unlock();
			}
		}
	}
	
	
	@Override
	public boolean add(E entry) {
		return putIfAbsent(entry) == null;
	}
		
	E put(E entry) {
		if (entry == null) {
			throw new NullPointerException();
		}
		return put(entry, true);
	}
	
	E putIfAbsent(E entry) {
		if (entry == null) {
			throw new NullPointerException();
		}
		return put(entry, false);
	}
	
	
	E put(E entry, boolean replaceIfPresent) {
		retry:
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			int dirSize = dir.length();
			long dirMask = dirSize - 1;
			long hashCode = entryAdapter.getLongHashCode(entry);
			int segmentIndex = (int)(hashCode & (long)dirMask);
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			if (seg.invalid) {
				seg.lock.unlock();
				continue retry;
			}
			try {
				/*
				 * try block encloses the segment lock; unlock is in the finally block
				 */
				if (seg.entryCount < loadThresholdLimit) {
					try {
						return seg.put(entry, hashCode, replaceIfPresent);
					} catch (SegmentOverflowException soe) {
						if (GATHER_EVENT_DATA) {
							forcedSplitCount.incrementAndGet();
						}
					}
				} else {
					if (GATHER_EVENT_DATA) {
						thresholdSplitCount.incrementAndGet();
					}
				}
				/*
				 * Either load factor was exceeded or the insertion threw 
				 * an overflow exception; split the segment.
				 */
				seg.invalid = true;
				Segment[] split = seg.split();
				if (GATHER_EVENT_DATA) {
					split[0].copyMetrics(seg);
				}
				E result;
				try {
					if (split[0].sharedBitsMatchSegment(hashCode)) {
						result = split[0].put(entry, hashCode, replaceIfPresent);
					} else if (split[1].sharedBitsMatchSegment(hashCode)) {
						result = split[1].put(entry, hashCode, replaceIfPresent);
					} else {
						throw new IllegalStateException("sharedBits conflict during segment split");
					}
				}
				catch (SegmentOverflowException soe1) {
					throw new IllegalStateException("sgement overflow occured after split");
				}
				updateDirectoryOnSplit(split);
				return result;
			}
			finally {
				seg.lock.unlock();
			}
		}
	}
	
	private void updateDirectoryOnSplit(Segment[] split) {
		dirLock.lock();
		try {
			segmentCount++; // doesn't need to be atomic; only modified under directory lock
			AtomicReferenceArray<Segment> dir = directory.get();
			int dirSize = dir.length();
			long dirMask = dirSize - 1;
			int depth = Long.bitCount(dirMask);
			/*
			 * refresh directory info now that it's locked
			 */
			if (depth < split[0].localDepth) {
				/*
				 * double directory size
				 */
				int newDirSize = dirSize * 2;
				if (newDirSize > MAX_DIR_SIZE) {
					throw new IllegalStateException("directory size limit exceeded");
				}
				AtomicReferenceArray<Segment> newDirectory = 
						new AtomicReferenceArray<Segment>(newDirSize);
				for (int i = 0; i < dirSize; i++) {
					newDirectory.set(i,dir.get(i));
					newDirectory.set(i+dirSize,dir.get(i));
				}
				dir = newDirectory;
				dirSize = newDirSize;
				directory.set(dir);
				dirMask = newDirSize - 1;
				depth = Long.bitCount(dirMask);
			}
			final int step = 1 << split[0].localDepth;
			for (int i = split[1].sharedBits; i < dirSize; i += step) {
				dir.set(i, split[1]);
			}
			for (int i = split[0].sharedBits; i < dirSize; i += step) {
				dir.set(i, split[0]);
			}
		}
		finally {
			dirLock.unlock();
		}

	}


	@Override
	public boolean contains(Object entryKey) {
		if (entryKey == null) {
			throw new NullPointerException();
		}
		return get(entryKey) != null;
	}
	
	@Override
	public E get(Object entryKey) {
		if (entryKey == null) {
			throw new NullPointerException();
		}
		AtomicReferenceArray<Segment> dir = directory.get();
		long dirMask = (long)(dir.length() - 1);
		long hashCode = entryAdapter.getLongHashCode(entryKey);
		Segment seg = dir.get((int)(hashCode & dirMask));
		return seg.get(entryKey, hashCode);
	}


	@Override
	public boolean isEmpty() {
		return mapEntryCount.get() == 0L;
	}


	@Override
	public Iterator<E> iterator() {
		return new EntryIterator();
	}


	@Override
	public long size() {
		return mapEntryCount.get();
	}
	
	/*
	 * An iterator over a map's segments, with the following behavior:
	 * 
	 * 1) The sequence of segments returned by the iterator is in ascending 
	 * directory index order (which is also ascending sharedBits order).
	 * 
	 * 2) The sequence of segments reflects a consistent, fully-populated
	 * directory at some point between the iterator's creation and its
	 * exhaustion. "Fully populated" means that the local depth and shared bits
	 * values for each segment, when mapped to directory indices, will 
	 * fill the directory. "Consistent" means that index will be mapped
	 * by one and only one segment.
	 * 
	 * 3) A segment is returned by next() at most once.
	 * 
	 * 4) If a segment is returned by next() and that segment is subsequently
	 * split before the iterator is exhausted, segments resulting from that
	 * split will not be returned by the iterator (see point 2 above; this
	 * is a criterion for consistency).
	 * 
	 */
	private class SegmentIterator {
		
		private final AtomicReferenceArray<Segment> dir;
		private final int dirSize;

		private Segment nextSegment;
		int nextSegmentIndex;
		BitSet markedSegments;

		private SegmentIterator() {
			dir = ConcurrentLargeHashSet.this.directory.get();
			dirSize = dir.length();
			markedSegments = new BitSet(dirSize);
			nextSegmentIndex = 0;
			nextSegment = getSegment();
			
		}
		
		private Segment getSegment() {
			while (nextSegmentIndex < dirSize) {
				/*
				 * markedSegments is a bitmap that is used to prevent
				 * traversals of segments that are split from segments
				 * that have already been traversed, to enforce the
				 * "at most once" behavior of iterators. If
				 * markedSegments[i] == true, don't traverse the 
				 * segment at directory[i];
				 */
				if (!markedSegments.get(nextSegmentIndex)) {
					Segment seg = dir.get(nextSegmentIndex);
					/*
					 * mark directory[i] for i = (nextSegmentIndex + n * 2^localDepth)
					 * for n = 0 ... log2(dirSize) - 1
					 */
					int step = 1 << seg.localDepth;
					for (int i = seg.sharedBits; i < dirSize; i += step) {
						markedSegments.set(i);
					}
					nextSegmentIndex++;
					return seg;
				} else {
					nextSegmentIndex++;
				}
			}
			return null;
		}


		private boolean hasNext() {
			return (nextSegment != null);
		}

		private Segment next() {
			if (nextSegment != null) {
				Segment current = nextSegment;
				nextSegment = getSegment();
				return current;
			} else {
				return null;
			}
		}		
	}
			
	private class EntryIterator implements Iterator<E> {
		
		private final SegmentIterator segIter;
		private Segment currentSegment;
		int nextBucketIndex;
		final E[] currentBucket;
		int currentBucketSize;
		int nextEntryIndex;
		
		@SuppressWarnings("unchecked")
		private EntryIterator() {
			segIter = new SegmentIterator();
			if (segIter.hasNext()) {
				currentSegment = segIter.next();
			}
			currentBucket = (E[])(new Object[HOP_RANGE]);
			nextBucketIndex = 0; // index in currentSegment of the next bucket
			currentBucketSize = 0;
			nextEntryIndex = 0; // index in currentBucket of the next entry
			getNextBucket();
		}
		
		private void getNextBucket() {
			while (currentSegment != null) {
				while (nextBucketIndex < segmentSize) {
					currentBucketSize = currentSegment.getBucket(nextBucketIndex++, currentBucket);
					if (currentBucketSize > 0) {
						nextEntryIndex = 0;
						return;
					}
				}
				if (segIter.hasNext()) {
					currentSegment = segIter.next();
				} else {
					currentSegment = null;
				}
				nextBucketIndex = 0;
			}
			currentBucketSize = 0;
		}
						
		@Override
		public E next() {
			if (currentBucketSize > 0) {
				E entry = currentBucket[nextEntryIndex++];
				/*
				 * Prime the iterator with the next entry, if there is one.
				 */
				if (nextEntryIndex >= currentBucketSize) {
					 getNextBucket();
				}
				return entry;
			} else {
				throw new NoSuchElementException();
			}
			
		}
		
		@Override
		public boolean hasNext() {
			return currentBucketSize > 0;	
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}		

	}

}
