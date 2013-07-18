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
package org.logicmill.util;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.logicmill.util.hash.SpookyHash64;

/** 
 * A concurrent hash map that scales well to large data sets.
 * <h3>Concurrency</h3>
 * All operations are thread-safe. Retrieval operations ({@code get}, 
 * {@code containsKey}, and iterator traversal) do not entail locking; they 
 * execute concurrently with update operations. Update operations ({@code put},
 * {@code putIfAbsent} and {@code remove}) may block if overlapping updates are
 * attempted on the same segment. Segmentation is configurable, so the 
 * probability of update blocking is (to a degree) under the programmer's 
 * control. Retrievals reflect the result of update operations that complete 
 * before the onset of the retrieval, and may also reflect the state 
 * of update operations that complete after the onset of the retrieval. 
 * Iterators do not throw {@link java.util.ConcurrentModificationException}.
 * If an entry is contained in the map prior to the iterator's creation and
 * it is not removed before the iterator is exhausted (that is, 
 * {@code hasNext()} returns {@code false}), it will be returned by the 
 * iterator. The entries returned by an iterator may or may not reflect updates
 * that occur after the iterator's creation.
 * Iterators themselves are not thread-safe; although iterators can be used 
 * concurrently with operations on the map and other iterators, an iterator 
 * should not be used by multiple threads.
 * 
 * <h3>Extendible Hashing</h3>
 * Extendible hashing 
 * <a href="#footnote-1">[1]</a> <a href="#footnote-2">[2]</a> allows the 
 * map to expand gracefully, amortizing the cost of resizing in constant-sized 
 * increments as the map grows. The map is partitioned into fixed-size 
 * segments. Hash values are mapped to segments through a central directory.
 * When a segment reaches the load factor threshold it splits into two 
 * segments. When a split would exceed directory capacity, the directory 
 * doubles in size. The current implementation does not merge segments to 
 * reduce capacity as entries are removed.
 * <h4>Concurrency during splits and directory expansion</h4>
 * When an update causes a segment to split, the updating thread will acquire
 * a lock on the directory to assign references to the new segments in the 
 * directory. If a split forces the directory to expand, the updating thread 
 * keeps the directory locked during expansion. A directory lock will not block 
 * a concurrent update unless that update forces a segment split.
 * <h3>Hopscotch Hashing</h3>
 * This implementation uses Hopscotch hashing <a href="#footnote-3">[3]</a> 
 * within segments for conflict resolution. Hopscotch hashing is similar to 
 * linear probing, except that colliding map entries all reside in relatively 
 * close proximity to each other, within a strictly bounded range, resulting 
 * in shorter searches and improved cache hit rates. Hopscotch hashing also 
 * yields good performance at load factors as high as 0.9.<p> 
 * At present, the hop range (longest distance from the hash index that
 * a collision can be stored) is set at 32, and the maximum search range
 * for an empty slot during an add operation is 512. If an add operation
 * fails because no empty slots are available in search range, or because 
 * hopping fails to free up a slot within hop range, the segment is split 
 * regardless of its current load factor. Such insertion failure splits 
 * typically don't occur unless the load factor exceeds 0.9.<p>
 * Hopscotch hashing was designed to support a high degree of concurrency, 
 * including non-blocking retrieval. This implementation follows the 
 * concurrency strategies described in the originating papers, with minor 
 * variations.
 * <h3>Long Hash Codes</h3>
 * ConcurrentLargeHashMap is designed to support hash maps that can expand to very 
 * large size (> 2<sup>32</sup> items). To that end, it uses 64-bit hash codes.
 * <h4>Hash function requirements</h4>
 * Because segment size is a power of 2, segment indices consist of bit fields
 * extracted directly from hash code values. It is important to choose a hash 
 * function that reliably exhibits avalanche and uniform distribution. An 
 * implementation of Bob Jenkins' SpookyHash V2 algorithm 
 * ({@link org.logicmill.util.hash.SpookyHash} and 
 * {@link org.logicmill.util.hash.SpookyHash64}) is available in conjunction 
 * with ConcurrentLargeHashMap, and is highly recommended.
 * <h4>Key adapters</h4>
 * Lacking a universally available 64-bit cognate of {@code Object.hashCode()},
 * some generalized means for obtaining 64-bit hash codes from keys must be
 * provided. <i>Key adapters</i> are helper classes that compute 64-bit hash 
 * codes for specific key types, allowing a class to be used as a key when it 
 * is not practical or possible to modify or extend the key class itself. Key
 * adapters implement the interface {@link LargeHashMap.KeyAdapter}{@code <K>}.
 * The map constructor accepts a key adapter as a parameter, causing that
 * adapter to be used by the map to obtain hash codes from keys. See
 * {@link #ConcurrentLargeHashMap(int, int, float, LargeHashMap.KeyAdapter)}
 * for details and an example key adapter.
 * <h4>Default key adapter</h4> 
 * If the key class implementation permits, it can provide a 64-bit hash code
 * directly by implementing the {@link LongHashable} interface. When the map
 * is constructed with a {@code null} key adapter, it uses a default key adapter 
 * implementation that will attempt to cast a key to {@code LongHashable} and 
 * obtain a 64-bit hash code by invoking {@code key.getLongHashCode()}. The 
 * default key adapter also handles keys of types {@code String} and 
 * {@code byte[]} without requiring a programmer-supplied key adapter. See
 * {@link #ConcurrentLargeHashMap(int, int, float, LargeHashMap.KeyAdapter)} for a
 * detailed discussion of the default key adapter.
 * <p id="footnote-1">[1] See 
 * <a href="http://dx.doi.org/10.1145%2F320083.320092"> Fagin, et al, 
 * "Extendible Hashing - A Fast Access Method for Dynamic Files", 
 * ACM TODS Vol. 4 No. 3, Sept. 1979</a> for the original article describing 
 * extendible hashing.</p>
 * <p id="footnote-2">[2] <a href="http://dl.acm.org/citation.cfm?id=588072">
 * Ellis, "Extendible Hashing for Concurrent Operations and Distributed Data", 
 * PODS 1983</a> describes strategies for concurrent operations on extendible 
 * hash tables. The strategy used in ConcurrentLargeHashMap is informed
 * by this paper, but does not follow it precisely.</p>
 * <p id="footnote-3">[3] 
 * <a href="http://mcg.cs.tau.ac.il/papers/disc2008-hopscotch.pdf">
 * Herlihy, et al, "Hopscotch Hashing", DISC 2008</a>.</p>
 * 
 * @author David Curtis
 * @see LargeHashMap
 * @see org.logicmill.util.hash.SpookyHash
 * @see org.logicmill.util.hash.SpookyHash64
 * 
 *
 * @param <K> type of keys stored in the map
 * @param <V> type of values stored in the map
 */
public class ConcurrentLargeHashMap<K, V> implements LargeHashMap<K, V> {
	
	/*
	 * Default entry implementation, stores hash codes to avoid repeatedly 
	 * computing them.
	 */
	private static class Entry<K,V> implements LargeHashMap.Entry<K, V> {
		private final K key;
		private final V value;
		private final long hashCode;
		
		private Entry(K key, V value, long hashCode) {
			this.key = key;
			this.value = value;
			this.hashCode = hashCode;
		}
		
		@Override
		public K getKey() {
			return key;
		}
		
		@Override
		public V getValue() {
			return value;
		}
		
		private long getHashCode() {
			return hashCode;
		}
		
		@Override
		public boolean equals(Object obj
				)  {
			if (obj instanceof ConcurrentLargeHashMap.Entry<?,?>) {
				@SuppressWarnings("unchecked")
				ConcurrentLargeHashMap.Entry<K, V> other = (ConcurrentLargeHashMap.Entry<K,V>) obj;
				return (this.hashCode == other.hashCode) 
						&& this.key.equals(other.key) 
						&& this.value.equals(other.value);
			} else {
				return false;
			}
			
		}
	}

	/*
	 * Default key adapter. 
	 */
	private static class KeyAdapter<K> implements LargeHashMap.KeyAdapter<K> {

		@Override
		public long getLongHashCode(K key) {
			if (key instanceof LongHashable) {
				return ((LongHashable)key).getLongHashCode();
			} else if (key instanceof CharSequence) {
				return SpookyHash64.hash((CharSequence)key, 0L);
			} else if (key instanceof byte[]) {
				return SpookyHash64.hash((byte[])key, 0L);
			} else {
				throw new IllegalArgumentException("key must be CharSequence, byte[], or instanceof LongHashable");
			}
		}
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
		private final AtomicReferenceArray<Entry<K,V>> entries;
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
			entries = new AtomicReferenceArray<Entry<K,V>>(segmentSize);
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
			
		private boolean sharedBitsMatchSegment(long hashValue) {
			return sharedBits(hashValue) == sharedBits;	
		}
		
		/* Splits a segment when it reaches the load threshold. Splitting 
		 * increases localDepth by one, so the new segments have sharedBits 
		 * values of (binary) 1??? and 0??? (where ??? is the sharedBits 
		 * value of the original segment). 
		 */
		@SuppressWarnings("unchecked")
		private Segment[] split() {
			Segment[] splitPair = new ConcurrentLargeHashMap.Segment[2];
			int  newLocalDepth = localDepth + 1;
			int newSharedBitMask = 1 << localDepth;
			splitPair[0] = new Segment(newLocalDepth, sharedBits);
			splitPair[1] = new Segment(newLocalDepth, sharedBits | newSharedBitMask);
			for (int i = 0; i < segmentSize; i++) {
				Entry<K,V> entry = entries.get(i);
				if (entry != null) {
					if ((entry.getHashCode() & (long)newSharedBitMask) == 0) {
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
		private void splitPut(Entry<K,V> entry) {
			// assert sharedBits(entry.getHashCode()) == sharedBits;
			try {
				placeWithinHopRange(bucketIndex(entry.getHashCode()), entry);
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
		void placeWithinHopRange(int bucketIndex, Entry<K,V> entry) 
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
		
		/*
		 * Implements replace(K key, V value) and replace (K key, V oldValue, V newValue).
		 * If oldValue is null, treat it as replace(key, value), otherwise, only replace
		 * if existing entry value equals oldValue.
		 */
		private V replace(K key, long hashCode, V oldValue, V newValue) {
			int bucketIndex = bucketIndex(hashCode);
			/*
			 * Search for entry with key
			 */
			int nextOffset = buckets.get(bucketIndex);
			while (nextOffset != NULL_OFFSET) {
				int entryIndex = wrapIndex(bucketIndex + nextOffset);
				Entry<K,V> entry = entries.get(entryIndex);
				if (entry.getHashCode() == hashCode  && entry.getKey().equals(key)) {
					if (oldValue != null && !
							oldValue.equals(entry.getValue())) {
						/*
						 * replace only if entry value equals oldValue
						 */
						return null;
					} else {
						Entry<K,V> newEntry = new Entry<K,V>(key, newValue, hashCode);
						entries.set(entryIndex, newEntry);
					}
					return entry.getValue();						
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
		private V put(K key, V value, long hashCode, boolean replaceIfPresent) 
		throws SegmentOverflowException {
			// assert sharedBits(hashValue) == sharedBits;
			int bucketIndex = bucketIndex(hashCode);
			/*
			 * Search for entry with key
			 */
			int nextOffset = buckets.get(bucketIndex);
			while (nextOffset != NULL_OFFSET) {
				int entryIndex = wrapIndex(bucketIndex + nextOffset);
				Entry<K,V> entry = entries.get(entryIndex);
				if (entry.getHashCode() == hashCode  && entry.getKey().equals(key)) {
					if (replaceIfPresent) {
						Entry<K,V> newEntry = new Entry<K,V>(key, value, hashCode);
						entries.set(entryIndex, newEntry);
					}
					return entry.getValue();						
				}
				nextOffset = offsets.get(entryIndex);
			}
			/*
			 *  Not found, insert the new entry.
			 */
			Entry<K,V> entry = new Entry<K,V>(key, value, hashCode);
			placeWithinHopRange(bucketIndex, entry);
			entryCount++;
			mapEntryCount.incrementAndGet();
			return null;
		}
	
		
		private V get(K key, long hashValue) {
			/*
			 * In very high update-rate environments, it might be
			 * necessary to limit re-tries, and go to a linear search of
			 * all offsets in {0 .. HOP_RANGE-1} if the limit is exceeded.
			 */
			int localRetrys = 0;
			// assert sharedBits(hashValue) == sharedBits;
			int bucketIndex = bucketIndex(hashValue);
			int oldTimeStamp, newTimeStamp = 0;
			try {
				retry:
				do {
					oldTimeStamp = timeStamps.get(bucketIndex);
					int nextOffset = buckets.get(bucketIndex);
					while (nextOffset != NULL_OFFSET) {
						int nextIndex = wrapIndex(bucketIndex + nextOffset);
						Entry<K,V> entry = entries.get(nextIndex);
						if (entry == null) {
							/*
							 * Concurrent update, re-try.
							 */
							localRetrys++;
							continue retry;
						}
						if (bucketIndex(entry.getHashCode()) != bucketIndex) {
							/*
							 * Concurrent update, re-try.
							 */
							localRetrys++;
							continue retry;
						}
						if (entry.getHashCode() == hashValue && entry.getKey().equals(key)) {
							return entry.getValue();
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
			Entry<K,V> adjacentEntry = entries.get(adjacentEntryIndex);
			if (adjacentEntry != null) {
				int adjacentEntryNextOffset = offsets.get(adjacentEntryIndex);
				if (adjacentEntryNextOffset != NULL_OFFSET) {
					int contractingBucketIndex = bucketIndex(adjacentEntry.getHashCode());
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
		private V remove(K key, long hashValue, V value) {
			// assert sharedBits(hashValue) == sharedBits;
			int bucketIndex = bucketIndex(hashValue);			
			V resultValue = null;
			int nextOffset = buckets.get(bucketIndex);
			if (nextOffset == NULL_OFFSET) {
				return null;
			}
			/*
			 * If the key is first in the bucket, it's a special case
			 * since the buckets array is updated.
			 */
			int nextIndex = wrapIndex(bucketIndex + nextOffset);
			Entry<K,V> entry = entries.get(nextIndex);
			// assert entry != null;
			if (entry.getHashCode() == hashValue && entry.getKey().equals(key)) {
				/*
				 * First entry in the bucket was the key to be removed.
				 */
				resultValue = entry.getValue();
				if (value != null && !value.equals(resultValue)) {
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
				return resultValue;
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
				entry = entries.get(nextIndex);
				if (entry.getHashCode() == hashValue && entry.getKey().equals(key)) {
					resultValue =  entry.getValue();
					if (value != null && !value.equals(resultValue)) {
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
					return resultValue;
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
		private int getBucket(int bucketIndex, Entry<K,V>[] bucketContents) {
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
						Entry<K,V> entry = entries.get(nextIndex);
						if (entry == null) {
							/*
							 * Concurrent update, try again.
							 */
							localRetrys++;
							continue retry;
						}
						if (bucketIndex(entry.getHashCode()) != bucketIndex) {
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

	private final LargeHashMap.KeyAdapter<K> keyAdapter;
	
	/** Creates a new, empty map with the specified segment size, initial 
	 * segment count, load threshold, and key adapter.
	 * <h4>Segmentation considerations</h4>
	 * By judiciously choosing segment size and initial segment count, the 
	 * programmer can make trade-offs that will affect performance. The following
	 * observations should be considered:
	 * <ul>
	 * <li>The initial segment count determines the initial limit of update 
	 * concurrency. As the map expands, so does the opportunity for update 
	 * concurrency.
	 * <li>The cost of a segment split, and the duration of the lock on a segment
	 * being split, are proportional to segment size.
	 * <li>Given a constant map growth rate, the frequency of segment splits is 
	 * inversely proportional to segment size.
	 * <li>For a given map size, directory size is inversely proportional 
	 * to segment size.
	 * <li>Given a constant map growth rate, the cost of directory expansion 
	 * doubles each time expansion occurs, but the expected time until the next
	 * expansion also doubles; the aggregate cost of directory expansion remains 
	 * constant, but it is paid in larger increments that happen less frequently.
	 * <li>Doubling the directory is less expensive than splitting a segment of 
	 * the same size. When a directory is doubled, the contents of the directory 
	 * array are simply duplicated into the top half of the new directory. 
	 * Splitting a segment requires insertion into the new segments, which may entail 
	 * collision resolution.
	 * <li>Contention for locks on the directory is not expected to be significant;
	 * most locks (excluding those involving directory expansion) are held only 
	 * briefly (just long enough to assign two references in the directory), and 
	 * directory locks block only updates that force segment splits.
	 * </ul>
	 * A proposed rule of thumb for determining segment size: If the expected 
	 * maximum map size is N, set segment size on the order of 
	 * &radic;<span style="text-decoration:overline;">&nbsp;N&nbsp;</span>. This
	 * results in roughly equal segment and directory sizes at expected capacity. 
	 * Note that, if the specified segment size is not a power of two, it will be 
	 * forced to the next largest power of two.
	 * <h4>Key adapters</h4> 
	 * To enable 64-bit hash codes, the programmer may provide a 
	 * <i>key adapter</i> class that implements
	 * {@link LargeHashMap.KeyAdapter}{@code <K>} for the key type {@code K}.
	 * For example, if it were necessary to use keys of type 
	 * {@link java.math.BigInteger}:<pre><code>
	 * ConcurrentLargeHashMap&lt;BigInteger, String&gt; map = 
	 * new ConcurrentLargeHashMap&lt;BigInteger, String&gt;(8192, 8, 0.8f, 
	 * 	new LargeHashMap.KeyAdapter&lt;BigInteger&gt;() {
	 * 		public long getHashCode(BigInteger key) {
	 * 			return org.logicmill.spookyhash.SpookyHash64.hash(key.toByteArray, 0L);
	 *		}
	 *	}
	 * );</code></pre>
	 * <h4>Default key adapter</h4>
	 * If {@code keyAadpter} is {@code null}, the map will use a default key 
	 * adapter. The default key adapter implementation attempts to do something 
	 * reasonable based on the run-time type of the key, accommodating the 
	 * following three use cases:
	 * <ul>
	 * <li> If the key can be cast to {@link LongHashable}, the default
	 * adapter method {@code getLongHashCode()} returns {@code 
	 * ((LongHashable)key).getLongHashCode()}.
	 * <li> If the key can be cast to {@link CharSequence}, the default
	 * adapter method {@code getLongHashCode()} returns {@code 
	 * SpookyHash64.hash((CharSequence)key, 0L)}.
	 * <li> If the key can be cast to {@code byte[]}, the default
	 * adapter method {@code getLongHashCode()} returns {@code 
	 * SpookyHash64.hash((byte[])key, 0L)}.
	 * </ul>
	 * Otherwise, any operation on the map that requires a key will throw
	 * an {@link IllegalArgumentException}.
	 * 
	 *  
	 * @param segSize size of segments, forced to the next largest power of two
	 * @param initSegCount number of segments created initially
	 * @param loadThreshold fractional threshold for map growth, observed at 
	 * the segment level
	 * @param keyAdapter adapter to generate 64-bit hash code values from keys,
	 * or {@code null} to employ the default key adapter
	 * 
	 * @see LargeHashMap.KeyAdapter
	 */
	public ConcurrentLargeHashMap(int segSize, int initSegCount, float loadThreshold, 
			LargeHashMap.KeyAdapter<K> keyAdapter) {
					
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
		
		int initDirCapacity = initSegCount;
		segmentCount = 0;
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
		
		this.keyAdapter = keyAdapter == null ? new KeyAdapter<K>() : keyAdapter;
	}
	
	

	/**
	 * {@inheritDoc}
	 */
	@Override
	public V putIfAbsent(K key, V value) {
		if (key == null || value == null) {
			throw new NullPointerException();
		}
		return put(key, value, false);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public V put(K key, V value) {
		if (key == null || value == null) {
			throw new NullPointerException();
		}
		return put(key, value, true);
	}
	
	private V put(K key, V value, boolean replaceIfPresent) {
		retry:
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			int dirSize = dir.length();
			long dirMask = dirSize - 1;
			long hashCode = keyAdapter.getLongHashCode(key);
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
						return seg.put(key, value, hashCode, replaceIfPresent);
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
				V result = null;
				try {
					if (split[0].sharedBitsMatchSegment(hashCode)) {
						result = split[0].put(key, value, hashCode, replaceIfPresent);
					} else if (split[1].sharedBitsMatchSegment(hashCode)) {
						result = split[1].put(key, value, hashCode, replaceIfPresent);
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
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public V get(K key) {
		if (key == null) {
			throw new NullPointerException();
		}
		AtomicReferenceArray<Segment> dir = directory.get();
		long dirMask = (long)(dir.length() - 1);
		long hashCode = keyAdapter.getLongHashCode(key);
		Segment seg = dir.get((int)(hashCode & dirMask));
		return seg.get(key, hashCode);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public V remove(K key) {
		if (key == null) {
			throw new NullPointerException();
		}
		long hashValue = keyAdapter.getLongHashCode(key);
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			int dirSize = dir.length();
			long dirMask = dirSize - 1;
			int segmentIndex = (int)(hashValue & (long)dirMask);
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return dir.get(segmentIndex).remove(key, hashValue, null);	
				}
			}
			finally {
				seg.lock.unlock();
			}
		}
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
			dir = ConcurrentLargeHashMap.this.directory.get();
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
			
	private class EntryIterator implements Iterator<LargeHashMap.Entry<K,V>> {
		
		private final SegmentIterator segIter;
		private Segment currentSegment;
		int nextBucketIndex;
		final Entry<K,V>[] currentBucket;
		int currentBucketSize;
		int nextEntryIndex;
		
		@SuppressWarnings("unchecked")
		private EntryIterator() {
			segIter = new SegmentIterator();
			if (segIter.hasNext()) {
				currentSegment = segIter.next();
			}
			currentBucket = new Entry[HOP_RANGE];
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
		public Entry<K,V> next() {
			if (currentBucketSize > 0) {
				Entry<K,V> entry = currentBucket[nextEntryIndex++];
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
	
	private class ValueIterator implements Iterator<V> {
		
		private final EntryIterator internalIterator;
		
		private ValueIterator() {
			internalIterator = new EntryIterator();
		}

		@Override
		public boolean hasNext() {
			return internalIterator.hasNext();
		}

		@Override
		public V next() {
			return internalIterator.next().getValue();
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}
	
	private class KeyIterator implements Iterator<K> {

		private final EntryIterator internalIterator;
		
		private KeyIterator() {
			internalIterator = new EntryIterator();
		}

		@Override
		public boolean hasNext() {
			return internalIterator.hasNext();
		}

		@Override
		public K next() {
			return internalIterator.next().getKey();
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}		
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterator<V> getValueIterator() {	
		return new ValueIterator();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterator<K> getKeyIterator() {
		return new KeyIterator();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterator<LargeHashMap.Entry<K,V>> getEntryIterator() {
		return new EntryIterator();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long size() {
		return mapEntryCount.get();
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsKey(K key) {
		if (key == null) {
			throw new NullPointerException();
		}
		return get(key) != null;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean remove(K key, V value) {
		if (key == null || value == null) {
			throw new NullPointerException();
		}
		long hashValue = keyAdapter.getLongHashCode(key);
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			int dirSize = dir.length();
			long dirMask = dirSize - 1;
			int segmentIndex = (int)(hashValue & (long)dirMask);
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return dir.get(segmentIndex).remove(key, hashValue, value) != null;	
				}
			}
			finally {
				seg.lock.unlock();
			}
		}
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public V replace(K key, V value) {
		if (key == null || value == null) {
			throw new NullPointerException();
		}
		long hashValue = keyAdapter.getLongHashCode(key);
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			long dirMask = dir.length() - 1;
			int segmentIndex = (int)(hashValue & (long)dirMask);
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return dir.get(segmentIndex).replace(key, hashValue, null, value);	
				}
			}
			finally {
				seg.lock.unlock();
			}
		}
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		if (key == null || oldValue == null || newValue == null) {
			throw new NullPointerException();
		}
		long hashValue = keyAdapter.getLongHashCode(key);
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			long dirMask = dir.length() - 1;
			int segmentIndex = (int)(hashValue & (long)dirMask);
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return dir.get(segmentIndex).replace(key, hashValue, oldValue, newValue) != null;	
				}
			}
			finally {
				seg.lock.unlock();
			}
		}
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isEmpty() {
		return mapEntryCount.get() == 0L;
	}

}