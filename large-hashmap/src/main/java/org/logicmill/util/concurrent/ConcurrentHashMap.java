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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

//import org.logicmill.util.LargeHashMap;
//import org.logicmill.util.LongHashable;

/** 
 * A concurrent hash map that scales well to large data sets.
 * <h3>Concurrency</h3>
 * All operations are thread-safe. Retrieval operations ({@code get}, 
 * {@code containsKey}, and iterator traversal) do not entail locking; they 
 * execute concurrently with update operations. Update operations ({@code put},
 * {@code putIfAbsent}, {@code replace} and {@code remove}) may block if 
 * overlapping updates are attempted on the same segment. Segmentation is 
 * configurable, so the probability of update blocking is (to a degree) under 
 * the programmer's control. Retrievals reflect the result of update operations 
 * that complete before the onset of the retrieval, and may also reflect the 
 * state of update operations that complete after the onset of the retrieval. 
 * Iterators do not throw {@link java.util.ConcurrentModificationException}.
 * If an entry is contained in the map prior to the iterator's creation and
 * it is not removed before the iterator is exhausted, it will be returned by 
 * the iterator. The entries returned by an iterator may or may not reflect 
 * updates that occur after the iterator's creation.
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
 * a concurrent update unless that update forces a segment split. Directory
 * locks do not affect retrievals.
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
 * ConcurrentHashMap is designed to support hash maps that can expand to very 
 * large size (> 2<sup>32</sup> items). To that end, it uses 64-bit hash codes.
 * <h4>Hash function requirements</h4>
 * Because segment size is a power of 2, segment indices consist of bit fields
 * extracted directly from hash code values. It is important to choose a hash 
 * function that reliably exhibits avalanche and uniform distribution. An 
 * implementation of Bob Jenkins' SpookyHash V2 algorithm 
 * ({@link org.logicmill.util.hash.SpookyHash} and 
 * {@link org.logicmill.util.hash.SpookyHash64}) is available in conjunction 
 * with ConcurrentHashMap, and is highly recommended.
 * <h4>Key adapters</h4>
 * {@code ConcurrentHashMap} uses key adapters (see {@link 
 * org.logicmill.util.LargeHashMap.KeyAdapter}) to obtain 64-bit hash codes 
 * from keys, and to perform matching comparisons on keys. A reference to a key 
 * adapter implementation can be passed as a parameter to the constructor 
 * {@link #ConcurrentHashMap(int, int, float, LargeHashMap.KeyAdapter)}.
 * {@code ConcurrentHashMap} also provides a default key adapter 
 * implementation that expects keys to implement {@link LongHashable}, and
 * uses {@code Object.equals()} for key matching. The default key adapter
 * is used when the map is constructed with 
 * {@link #ConcurrentHashMap(int, int, float)}.
 * <p id="footnote-1">[1] See 
 * <a href="http://dx.doi.org/10.1145%2F320083.320092"> Fagin, et al, 
 * "Extendible Hashing - A Fast Access Method for Dynamic Files", 
 * ACM TODS Vol. 4 No. 3, Sept. 1979</a> for the original article describing 
 * extendible hashing.</p>
 * <p id="footnote-2">[2] <a href="http://dl.acm.org/citation.cfm?id=588072">
 * Ellis, "Extendible Hashing for Concurrent Operations and Distributed Data", 
 * PODS 1983</a> describes strategies for concurrent operations on extendible 
 * hash tables. The strategy used in ConcurrentHashMap is informed
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
public class ConcurrentHashMap<K, V> implements ConcurrentMap<K, V>, Serializable {
	
	
    /**
	 * 
	 */
	private static final long serialVersionUID = -3876715633165576706L;


	/**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     */
    private static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }
	
	/*
	 * Default entry implementation, stores hash codes to avoid repeatedly 
	 * computing them.
	 */
	private static class HashEntry<K,V> implements Map.Entry<K, V>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4898232051318565333L;
		private final K key;
		private final V value;
		private transient final int keyHashCode;
		
		private HashEntry(K key, V value, int keyHashCode) {
			this.key = key;
			this.value = value;
			this.keyHashCode = keyHashCode;
		}
		
		private HashEntry(K key, V value) {
			this(key, value, hash(key.hashCode()));
		}
		
		private HashEntry(Map.Entry<? extends K, ? extends V> e) {
			this(e.getKey(), e.getValue());
		}
		
		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Map.Entry) {
				Map.Entry e = (Map.Entry) obj;
				return key.equals(e.getKey()) && value.equals(e.getValue());
			} else {
				return false;
			}
		}
		
		@Override
		public int hashCode() {
			return key.hashCode() ^ value.hashCode();
		}
		
		private int getKeyHashCode() {
			return keyHashCode;
		}

		@Override
		public V setValue(V value) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public String toString() {
			return key + "=" + value;
		}
		
		private void writeObject(ObjectOutputStream s) throws IOException {
			s.defaultWriteObject();
		}
		
		private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
			s.defaultReadObject();
			Field f;
			try {
				f = this.getClass().getDeclaredField("keyHashCode");
				f.setAccessible(true);
				f.setInt(this, hash(key.hashCode()));
			} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
				assert false : e.getMessage() + "should never happen";
			}
		}
	}

	/*
	 * Default key adapter. 
	 */
/*	private static class DefaultKeyAdapter<K> implements LargeHashMap.KeyAdapter<K> {

		@Override
		public long getLongHashCode(Object key) {
			if (key instanceof LongHashable) {
				return ((LongHashable)key).getLongHashCode();
			} else {
				throw new IllegalArgumentException("key must implement org.logicmill.util.LongHashable");
			}
		}

		@Override
		public boolean keyMatches(K mapKey, Object key) {
			return mapKey.equals(key);
		}
	}
*/	
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
	 * 		bucketIndex = (keyHashCode >>> localDepth) & indexMask;  
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
	 * 			HashEntry firstEntry = entries[index];
	 * 			...
	 * 
	 * Subsequent entries in the bucket by following the links in offsets:
	 * 
	 * 		while (offsets[index] != NULL_OFFSET) {
	 * 			nextIndex = (bucketIndex + offsets[index]) & indexMask;
	 * 			HashEntry nextEntry = entries[nextIndex];
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
		 * ConcurrentHashMapProbe.SegmentProbe, with reflection, so there
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
		private final int sharedBitsMask;
		private final int indexMask;
		
		/*
		 * Segment arrays, discussed in detail above. Note: these
		 * are atomic only because Java doesn't support arrays of 
		 * volatile types. Atomicity isn't needed, (except in the 
		 * case of timeStamps) since modifications are always protected 
		 * by segment locks, but guaranteeing order of visibility is 
		 * critical.
		 */
		private final AtomicReferenceArray<HashEntry<K,V>> entries;
		private final AtomicIntegerArray offsets;
		private final AtomicIntegerArray buckets;
		private final AtomicIntegerArray timeStamps;

		/*
		 * entryCount is only modified during a segment lock, 
		 * so it's volatile instead of atomic
		 */
		private volatile int entryCount;
		
		/*
		 * Prevents an updating thread from modifying a segment
		 * that has already been removed from the directory.
		 */
		private boolean invalid;
		
		private final ReentrantLock lock;
		
		
		/*
		 * Accumulated concurrency event metrics, for reporting by
		 * ConcurrentHashMapInspector (the which see for a detailed 
		 * discussion of these values).
		 * 
		 * The retry values are incremented during read operations,
		 * so they're not protected by a lock. The recycle values
		 * are lock-protected. They probably don't need to be volatile,
		 * but what the hell.
		 */
		
		/*
		 * TODO: revisit the need for long values of accumulated counts
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
			sharedBitsMask = (1 << localDepth) - 1;
			indexMask = segmentSize - 1;
			offsets = new AtomicIntegerArray(segmentSize);
			buckets = new AtomicIntegerArray(segmentSize);
			entries = new AtomicReferenceArray<HashEntry<K,V>>(segmentSize);
			timeStamps = new AtomicIntegerArray(segmentSize);
			for (int i = 0; i < segmentSize; i++) {
				// TODO: fix the interpretation of bucket lists so zero is NULL
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
			
		private boolean sharedBitsMatchSegment(int hashValue) {
			return sharedBits(hashValue) == sharedBits;	
		}
		
		/* Splits a segment when it reaches the load threshold. Splitting 
		 * increases localDepth by one, so the new segments have sharedBits 
		 * values of (binary) 1??? and 0??? (where ??? is the sharedBits 
		 * value of the original segment). 
		 */
		@SuppressWarnings("unchecked")
		private Segment[] split() {
			Segment[] splitPair = new ConcurrentHashMap.Segment[2];
			int  newLocalDepth = localDepth + 1;
			int newSharedBitMask = 1 << localDepth;
			splitPair[0] = new Segment(newLocalDepth, sharedBits);
			splitPair[1] = new Segment(newLocalDepth, sharedBits | newSharedBitMask);
			for (int i = 0; i < segmentSize; i++) {
				HashEntry<K,V> entry = entries.get(i);
				if (entry != null) {
					if ((entry.getKeyHashCode() & newSharedBitMask) == 0) {
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
		private int bucketIndex(int hashCode) {
			return (hashCode >>> localDepth) & indexMask;
		}
		
		private int sharedBits(int hashCode) {
			return hashCode & sharedBitsMask;
		}
		
		private int wrapIndex(int unwrappedIndex) {
			return unwrappedIndex & indexMask;
		}
		
		/*
		 * splitPut(HashEntry<K,V>) is only used during a split, which allows 
		 * some simplifying assumptions to be made, to wit:
		 * 	1) overflow shouldn't happen,
		 * 	2) the target segment is visible only to the current thread, and
		 * 	2) the key/entry can't already be in the segment.
		 */
		private void splitPut(HashEntry<K,V> entry) {
			// assert sharedBits(entry.getHashCode()) == sharedBits;
			try {
				placeWithinHopRange(bucketIndex(entry.getKeyHashCode()), entry);
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
		void placeWithinHopRange(int bucketIndex, HashEntry<K,V> entry) 
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
				final int swapSlotOffset = buckets.get(swapBucketIndex);
				if (swapSlotOffset != NULL_OFFSET && swapSlotOffset < freeSlotOffset) {
					final int swapSlotIndex = wrapIndex(swapBucketIndex + swapSlotOffset);
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
		 * Implements replace(Object key, V value) and replace (Object key, V oldValue, V newValue).
		 * If oldValue is null, treat it as replace(key, value), otherwise, only replace
		 * if existing entry value equals oldValue.
		 */
		private V replace(K key, int hashCode, V oldValue, V newValue) {
			int bucketIndex = bucketIndex(hashCode);
			/*
			 * Search for entry with key
			 */
			int nextOffset = buckets.get(bucketIndex);
			while (nextOffset != NULL_OFFSET) {
				int entryIndex = wrapIndex(bucketIndex + nextOffset);
				HashEntry<K,V> entry = entries.get(entryIndex);
				if (entry.getKeyHashCode() == hashCode  && key.equals(entry.getKey())) {
					if (oldValue != null && !
							oldValue.equals(entry.getValue())) {
						/*
						 * replace only if entry value equals oldValue
						 */
						return null;
					} else {
						HashEntry<K,V> newEntry = new HashEntry<K,V>(key, newValue, hashCode);
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
		private V put(K key, V value, int hashCode, boolean replaceIfPresent) 
		throws SegmentOverflowException {
			// assert sharedBits(hashValue) == sharedBits;
			int bucketIndex = bucketIndex(hashCode);
			/*
			 * Search for entry with key
			 */
			int nextOffset = buckets.get(bucketIndex);
			while (nextOffset != NULL_OFFSET) {
				int entryIndex = wrapIndex(bucketIndex + nextOffset);
				HashEntry<K,V> entry = entries.get(entryIndex);
				if (entry.getKeyHashCode() == hashCode && key.equals(entry.getKey())) {
					if (replaceIfPresent) {
						HashEntry<K,V> newEntry = new HashEntry<K,V>(key, value, hashCode);
						entries.set(entryIndex, newEntry);
					}
					return entry.getValue();						
				}
				nextOffset = offsets.get(entryIndex);
			}
			/*
			 *  Not found, insert the new entry.
			 */
			HashEntry<K,V> entry = new HashEntry<K,V>(key, value, hashCode);
			placeWithinHopRange(bucketIndex, entry);
			entryCount++;
			mapEntryCount.incrementAndGet();
			return null;
		}
		
		/*
		 * only called by readObject during de-serialization.
		 */
		private void put(HashEntry<K,V> newEntry) throws SegmentOverflowException {
			// assert sharedBits(hashValue) == sharedBits;
			int hashCode = newEntry.getKeyHashCode();
			int bucketIndex = bucketIndex(hashCode);
			/*
			 * Search for entry with key
			 */
			int nextOffset = buckets.get(bucketIndex);
			while (nextOffset != NULL_OFFSET) {
				int entryIndex = wrapIndex(bucketIndex + nextOffset);
				HashEntry<K,V> mappedEntry = entries.get(entryIndex);
				if (mappedEntry.getKeyHashCode() == hashCode && newEntry.getKey().equals(mappedEntry.getKey())) {
					throw new IllegalStateException("duplicate entry key encountered during de-serialization");
				}
				nextOffset = offsets.get(entryIndex);
			}
			/*
			 *  Not found, insert the new entry.
			 */
			placeWithinHopRange(bucketIndex, newEntry);
			entryCount++;
			mapEntryCount.incrementAndGet();
		}
	
		
		private V get(Object key, int hashValue) {
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
						HashEntry<K,V> entry = entries.get(nextIndex);
						if (entry == null) {
							/*
							 * Concurrent update, re-try.
							 */
							localRetrys++;
							continue retry;
						}
						if (bucketIndex(entry.getKeyHashCode()) != bucketIndex) {
							/*
							 * Concurrent update, re-try.
							 */
							localRetrys++;
							continue retry;
						}
						if (entry.getKeyHashCode() == hashValue && key.equals(entry.getKey())) {
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
			HashEntry<K,V> adjacentEntry = entries.get(adjacentEntryIndex);
			if (adjacentEntry != null) {
				int adjacentEntryNextOffset = offsets.get(adjacentEntryIndex);
				if (adjacentEntryNextOffset != NULL_OFFSET) {
					int contractingBucketIndex = bucketIndex(adjacentEntry.getKeyHashCode());
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
		private V remove(Object key, int hashValue, Object value) {
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
			HashEntry<K,V> entry = entries.get(nextIndex);
			// assert entry != null;
			if (entry.getKeyHashCode() == hashValue && key.equals(entry.getKey())) {
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
				if (entry.getKeyHashCode() == hashValue && key.equals(entry.getKey())) {
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
		private int getBucket(int bucketIndex, HashEntry<K,V>[] bucketContents) {
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
						HashEntry<K,V> entry = entries.get(nextIndex);
						if (entry == null) {
							/*
							 * Concurrent update, try again.
							 */
							localRetrys++;
							continue retry;
						}
						if (bucketIndex(entry.getKeyHashCode()) != bucketIndex) {
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
	private static final int ADD_RANGE = 512;
	/*
	 * Null values for buckets and hops array indices in Segment,
	 * since zero is a valid offset and index.
	 */
	private static final int NULL_OFFSET = -1;
	private static final int NULL_INDEX = -1;

	/*
	 * Minimum and maximum configuration values are for basic sanity checks
	 * during construction.
	 */
	private static final int MAX_CAPACITY = 1 << 30;
	private static final int MIN_SEGMENT_SIZE = 1024;
	private static final int MIN_INITIAL_SEGMENT_COUNT = 2;
	private static final int MIN_INITIAL_CAPACITY = MIN_SEGMENT_SIZE * MIN_INITIAL_SEGMENT_COUNT;
	private static final float MIN_LOAD_THRESHOLD = 0.1f;
	private static final float MAX_LOAD_THRESHOLD = 1.0f;
	private static final float DEFAULT_LOAD_THRESHOLD = 0.8f;
	private static final int MAX_SEGMENT_SIZE = MAX_CAPACITY / MIN_INITIAL_SEGMENT_COUNT;
	private static final int MAX_DIR_SIZE = MAX_CAPACITY / MIN_SEGMENT_SIZE;
			
	/*
	 * initial config values, handled by default serialization
	 */
	private final int segmentSize;
	private final int maxSegmentCount;
	private final int loadThresholdLimit;
	private final int initSegmentCount;
	
	/*
	 * transient/final, must be initialized with reflection in readObject
	 */
	private transient final ReentrantLock dirLock;
	private transient final AtomicReference<AtomicReferenceArray<Segment>> directory;
	private transient volatile int segmentCount;	
	private transient final AtomicInteger forcedSplitCount;
	private transient final AtomicInteger thresholdSplitCount;
	private transient final AtomicInteger segmentSerialID;	
	private transient final AtomicInteger mapEntryCount;
	
	/*
	 * initialize to null
	 */	
    private transient Set<K> keySet;
    private transient Set<Map.Entry<K,V>> entrySet;
    private transient Collection<V> values;

	
	private static class SegmentOverflowException extends Exception {
		private static final long serialVersionUID = -5917984727339916861L;	
	}	

//	private final LargeHashMap.KeyAdapter<K> keyAdapter;
	
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
	 * <li>The cost of a segment split and the duration of the lock on a segment
	 * being split are proportional to segment size.
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
	 *  
	 * @param segSize size of segments, forced to the next largest power of two
	 * @param initSegCount number of segments created initially
	 * @param loadThreshold fractional threshold for map growth, observed at 
	 * the segment level
	 * @param keyAdapter key adapter to be used by this map instance
	 * 
	 * @see org.logicmill.util.LargeHashMap.KeyAdapter
	 */
	public ConcurrentHashMap(int segSize, int initSegs, float load) {
		this(MapConfig.create()
				.withSegmentSize(segSize)
				.withInitSegmentCount(initSegs)
				.withLoadFactor(load).build());
	}
	
	public ConcurrentHashMap(MapConfig config) {
		maxSegmentCount = config.getMaxSegmentCount();
		initSegmentCount = config.getInitSegments();
		segmentSize = config.getSegmentSize();
		loadThresholdLimit = (int)(((float)segmentSize) * config.getLoadFactor());
		
		segmentCount = config.getInitSegments();
		
		forcedSplitCount = new AtomicInteger(0);
		thresholdSplitCount = new AtomicInteger(0);
		segmentSerialID = new AtomicInteger(0);
		int dirSize = segmentCount;
		int dirMask = dirSize - 1;
		int depth = Integer.bitCount(dirMask);
		AtomicReferenceArray<Segment> dir = 
				new AtomicReferenceArray<Segment>(segmentCount);
		for (int i = 0; i < segmentCount; i++) {
			dir.set(i, new Segment(depth, i));
		}
		dirLock = new ReentrantLock(true);
		directory = new AtomicReference<AtomicReferenceArray<Segment>>(dir);
		mapEntryCount = new AtomicInteger(0);		
		
	}
	

	/** Creates a new, empty map with the specified segment size, initial 
	 * segment count, load threshold, and a default key adapter 
	 * implementation (see {@link 
	 * #ConcurrentHashMap(int, int, float, LargeHashMap.KeyAdapter)} for
	 * a discussion of segmentation issues).
	 * <h4>Default key adapter</h4>
	 * The default key adapter implementation expects key objects to implement
	 * {@link LongHashable}. Specifically:
	 * <ul>
	 * <li> If the key can be cast to {@link LongHashable}, the default
	 * key adapter implementation of {@code getLongHashCode()} returns {@code 
	 * ((LongHashable)key).getLongHashCode()}. Otherwise, a
	 * {@code ClassCastException} is thrown.
	 * <li> The default key adapter implementation of 
	 * {@code keyMatches(K mappedKey, Object key)} delegates to {@code
	 * mappedKey.equals(key)}.
	 * </ul>
	 * @param segSize size of segments, forced to the next largest power of two
	 * @param initSegCount number of segments created initially
	 * @param loadThreshold fractional threshold for map growth, observed at 
	 * the segment level
	 */
/*	public ConcurrentHashMap(int segSize, int initSegCount, float loadThreshold) {
		this(segSize, initSegCount, loadThreshold, new DefaultKeyAdapter<K>());
	}
*/
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
	
	/*
	 * should only be invoked during de-serialization, so it doesn't
	 * try to accommodate concurrency.
	 */
	private void put(HashEntry<K,V> entry) {
		AtomicReferenceArray<Segment> dir = directory.get();
		int dirSize = dir.length();
		int dirMask = dirSize - 1;
		int hashCode = entry.getKeyHashCode();
		int segmentIndex = hashCode & dirMask;
		Segment seg = dir.get(segmentIndex);

		if (seg.entryCount < loadThresholdLimit) {
			try {
				seg.put(entry);
				return;
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
		seg.invalid = true; // probably unnecessary
		Segment[] split = seg.split();
		if (GATHER_EVENT_DATA) {
			split[0].copyMetrics(seg);
		}
		V result;
		try {
			if (split[0].sharedBitsMatchSegment(hashCode)) {
				split[0].put(entry);
			} else if (split[1].sharedBitsMatchSegment(hashCode)) {
				split[1].put(entry);
			} else {
				throw new IllegalStateException("sharedBits conflict during segment split");
			}
		}
		catch (SegmentOverflowException soe1) {
			throw new IllegalStateException("sgement overflow occured after split");
		}
		updateDirectoryOnSplit(split);		
	}
	
	private V put(K key, V value, boolean replaceIfPresent) {
		retry:
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			int dirSize = dir.length();
			int dirMask = dirSize - 1;
			int hashCode = hash(key.hashCode());
			int segmentIndex = hashCode & dirMask;
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
				V result;
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
			int dirMask = dirSize - 1;
			int depth = Integer.bitCount(dirMask);
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
				depth = Integer.bitCount(dirMask);
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
	public V get(Object key) {
		if (key == null) {
			throw new NullPointerException();
		}
		AtomicReferenceArray<Segment> dir = directory.get();
		int dirMask = dir.length() - 1;
		int hashCode = hash(key.hashCode());
		Segment seg = dir.get(hashCode & dirMask);
		return seg.get(key, hashCode);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public V remove(Object key) {
		if (key == null) {
			throw new NullPointerException();
		}
		int hashValue = hash(key.hashCode());
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			int dirSize = dir.length();
			int dirMask = dirSize - 1;
			int segmentIndex = hashValue & dirMask;
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return seg.remove(key, hashValue, null);	
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
			dir = ConcurrentHashMap.this.directory.get();
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
			
	private class HashEntryIterator implements Iterator<HashEntry<K,V>> {
		
		private final SegmentIterator segIter;
		private Segment currentSegment;
		int nextBucketIndex;
		final HashEntry<K,V>[] currentBucket;
		int currentBucketSize;
		int nextEntryIndex;
		private HashEntry<K,V> lastEntry;
		
		@SuppressWarnings("unchecked")
		private HashEntryIterator() {
			segIter = new SegmentIterator();
			if (segIter.hasNext()) {
				currentSegment = segIter.next();
			}
			currentBucket = new HashEntry[HOP_RANGE];
			nextBucketIndex = 0; // index in currentSegment of the next bucket
			currentBucketSize = 0;
			nextEntryIndex = 0; // index in currentBucket of the next entry
			getNextBucket();
			lastEntry = null;
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
		public HashEntry<K,V> next() {
			if (currentBucketSize > 0) {
				HashEntry<K,V> entry = currentBucket[nextEntryIndex++];
				/*
				 * Prime the iterator with the next entry, if there is one.
				 */
				if (nextEntryIndex >= currentBucketSize) {
					 getNextBucket();
				}
				lastEntry = entry;
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
			if (lastEntry == null) {
				throw new IllegalStateException();
			} else {
				ConcurrentHashMap.this.remove(lastEntry.getKey());
				lastEntry = null;
			}
		}		

	}
	
	
	/**
	 * {@inheritDoc}
	 */
	public Iterator<HashEntry<K,V>> getHashEntryIterator() {
		return new HashEntryIterator();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {
		return mapEntryCount.get();
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsKey(Object key) {
		if (key == null) {
			throw new NullPointerException();
		}
		return get(key) != null;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean remove(Object key, Object value) {
		if (key == null || value == null) {
			throw new NullPointerException();
		}
		int hashValue = hash(key.hashCode());
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			int dirSize = dir.length();
			int dirMask = dirSize - 1;
			int segmentIndex = hashValue & dirMask;
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return seg.remove(key, hashValue, value) != null;	
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
		int hashValue = hash(key.hashCode());
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			int dirMask = dir.length() - 1;
			int segmentIndex = hashValue & dirMask;
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return seg.replace(key, hashValue, null, value);	
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
		int hashValue = hash(key.hashCode());
		while (true) {
			AtomicReferenceArray<Segment> dir = directory.get();
			int dirMask = dir.length() - 1;
			int segmentIndex = hashValue & dirMask;
			Segment seg = dir.get(segmentIndex);
			seg.lock.lock();
			try {
				if (!seg.invalid) {
					return seg.replace(key, hashValue, oldValue, newValue) != null;	
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

	@Override
	public boolean containsValue(Object value) {
		return get(value) != null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
			put(e.getKey(), e.getValue());
		}
	}

	@Override
	public void clear() {
		HashEntryIterator iter = new HashEntryIterator();
		while (iter.hasNext()) {
			iter.next();
			iter.remove();
		}
	}

	final class EntryIterator
	implements Iterator<Entry<K,V>>
	{
		private HashEntryIterator iter = new HashEntryIterator();

		public Map.Entry<K,V> next() {
			return iter.next();
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public void remove() {
			iter.remove();
		}
	}

    final class KeyIterator
        implements Iterator<K>, Enumeration<K>
    {
    	private final HashEntryIterator entryIter;
    	
    	private KeyIterator() {
    		entryIter = new HashEntryIterator();
    	}

		@Override
		public boolean hasMoreElements() {
			return hasNext();
		}

		@Override
		public K nextElement() {
			return next();
		}

		@Override
		public boolean hasNext() {
			return entryIter.hasNext();
		}

		@Override
		public K next() {
			return entryIter.next().getKey();
		}

		@Override
		public void remove() {
			entryIter.remove();
		}
    }

    final class ValueIterator
    	implements Iterator<V>, Enumeration<V> {

    	private final HashEntryIterator entryIter;

    	private ValueIterator() {
    		entryIter = new HashEntryIterator();
    	}
    	
		@Override
		public boolean hasMoreElements() {
			return hasNext();
		}

		@Override
		public V nextElement() {
			return next();
		}

		@Override
		public boolean hasNext() {
			return entryIter.hasNext();
		}

		@Override
		public V next() {
			return entryIter.next().getValue();
		}

		@Override
		public void remove() {
			entryIter.remove();
		}
    	
    }
	
    final class KeySet extends AbstractSet<K> {
        public Iterator<K> iterator() {
            return new KeyIterator();
        }
        public int size() {
            return ConcurrentHashMap.this.size();
        }
        public boolean isEmpty() {
            return ConcurrentHashMap.this.isEmpty();
        }
        public boolean contains(Object o) {
            return ConcurrentHashMap.this.containsKey(o);
        }
        public boolean remove(Object o) {
            return ConcurrentHashMap.this.remove(o) != null;
        }
        public void clear() {
            ConcurrentHashMap.this.clear();
        }
    }
    
    final class Values extends AbstractCollection<V> {
        public Iterator<V> iterator() {
            return new ValueIterator();
        }
        public int size() {
            return ConcurrentHashMap.this.size();
        }
        public boolean isEmpty() {
            return ConcurrentHashMap.this.isEmpty();
        }
        public boolean contains(Object o) {
            return ConcurrentHashMap.this.containsValue(o);
        }
        public void clear() {
            ConcurrentHashMap.this.clear();
        }
    }


    final class EntrySet extends AbstractSet<Map.Entry<K,V>> {
        public Iterator<Map.Entry<K,V>> iterator() {
            return new EntryIterator();
        }
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?,?> e = (Map.Entry<?,?>)o;
            V v = ConcurrentHashMap.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }
        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?,?> e = (Map.Entry<?,?>)o;
            return ConcurrentHashMap.this.remove(e.getKey(), e.getValue());
        }
        public int size() {
            return ConcurrentHashMap.this.size();
        }
        public boolean isEmpty() {
            return ConcurrentHashMap.this.isEmpty();
        }
        public void clear() {
            ConcurrentHashMap.this.clear();
        }
    }

	
	@Override
	public Set<K> keySet() {
        Set<K> ks = keySet;
        return (ks != null) ? ks : (keySet = new KeySet());
	}

	@Override
	public Collection<V> values() {
        Collection<V> vs = values;
        return (vs != null) ? vs : (values = new Values());
	}

	@Override
    public Set<Map.Entry<K,V>> entrySet() {
        Set<Map.Entry<K,V>> es = entrySet;
        return (es != null) ? es : (entrySet = new EntrySet());
    }
	
	@Override 
    public String toString() {
        Iterator<Entry<K,V>> i = entrySet().iterator();
        if (! i.hasNext())
            return "{}";

        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (;;) {
            Entry<K,V> e = i.next();
            K key = e.getKey();
            V value = e.getValue();
            sb.append(key   == this ? "(this Map)" : key);
            sb.append('=');
            sb.append(value == this ? "(this Map)" : value);
            if (! i.hasNext())
                return sb.append('}').toString();
            sb.append(", ");
        }
    }
	
	@Override
	public int hashCode() {
		int h = 0;
		Iterator<HashEntry<K,V>> i = getHashEntryIterator();
		while (i.hasNext()) {
			h += i.next().hashCode();
		}
		return h;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof Map)) {
			return false;
		}
		Map<K,V> m = (Map<K,V>) obj;
		if (m.size() != size()) {
			return false;
		}
		try {
			Iterator<Entry<K,V>> i = entrySet().iterator();
			while (i.hasNext()) {
				Entry<K,V> e = i.next();
				K key = e.getKey();
				V value = e.getValue();
				Object mVal = m.get(key);
				if (mVal == null) {
					return false;
				}
				if (!value.equals(mVal)) {
					return false;
				}
			}
		} catch (ClassCastException | NullPointerException ignore) {
			return false;
		}
		return true;
	}
	
	private void writeObject(ObjectOutputStream s) throws IOException {
		s.defaultWriteObject();
		Iterator<HashEntry<K,V>> iter = getHashEntryIterator();
		while (iter.hasNext()) {
			s.writeObject(iter.next());
		}
		s.writeObject(null);
	}
	
	private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
		s.defaultReadObject();
		
		Field f;
		try {
			Class<? extends ConcurrentHashMap> clazz = this.getClass();
			f = clazz.getDeclaredField("segmentCount");
			f.setAccessible(true);
			f.setInt(this, initSegmentCount);
			
			f = clazz.getDeclaredField("forcedSplitCount");
			f.setAccessible(true);
			f.set(this, new AtomicInteger(0));
			
			f = clazz.getDeclaredField("thresholdSplitCount");
			f.setAccessible(true);
			f.set(this, new AtomicInteger(0));
			
			f = clazz.getDeclaredField("segmentSerialID");
			f.setAccessible(true);
			f.set(this, new AtomicInteger(0));
			
			f = clazz.getDeclaredField("mapEntryCount");
			f.setAccessible(true);
			f.set(this, new AtomicInteger(0));
			
			f = clazz.getDeclaredField("dirLock");
			f.setAccessible(true);
			f.set(this, new ReentrantLock(true));
			
			int dirSize = segmentCount;
			int dirMask = dirSize - 1;
			int depth = Integer.bitCount(dirMask);
			AtomicReferenceArray<Segment> dir = 
					new AtomicReferenceArray<Segment>(segmentCount);
			for (int i = 0; i < segmentCount; i++) {
				dir.set(i, new Segment(depth, i));
			}
			
			f = clazz.getDeclaredField("directory");
			f.setAccessible(true);
			f.set(this, new AtomicReference<AtomicReferenceArray<Segment>>(dir));
			
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			assert false : e.getMessage() + "should never happen";
		}
		
		while (true) {
			HashEntry<K,V> entry = (HashEntry<K,V>) s.readObject();
			if (entry == null) {
				break;
			}
			put(entry);
		}	
	}
	
	
	// TODO: implement serialization, equals, hashCode clone?
	
	public static class MapConfig {
		private final int segmentSize;
		private final int initSegments;
		private final int maxSegmentCount;
		private final float loadFactor;
		private MapConfig(int segSize, int initSegs, int maxSegs, float factor) {
			segmentSize = segSize;
			initSegments = initSegs;
			maxSegmentCount = maxSegs;
			loadFactor = factor;
		}
		public int getSegmentSize() {
			return segmentSize;
		}
		public int getInitSegments() {
			return initSegments;
		}
		public int getMaxSegmentCount() {
			return maxSegmentCount;
		}	
		public float getLoadFactor() {
			return loadFactor;
		}
		public static MapConfigBuilder create() {
			return new MapConfigBuilder();
		}

	}
	
	
	public static class MapConfigBuilder {
		
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
		
		private static int adjustSegmentSize(int segSize) {
			segSize = nextPowerOfTwo(segSize);
			if (segSize > MAX_SEGMENT_SIZE) {
				segSize = MAX_SEGMENT_SIZE;
			}
			if (segSize < MIN_SEGMENT_SIZE) {
				segSize = MIN_SEGMENT_SIZE;
			}
			return nextPowerOfTwo(segSize);			
		}
		
		private static float adjustLoadFactor(float factor) {
			if (factor > MAX_LOAD_THRESHOLD) {
				factor = MAX_LOAD_THRESHOLD;
			}
			if (factor < MIN_LOAD_THRESHOLD) {
				factor = MIN_LOAD_THRESHOLD;
			}
			return factor;
			
		}
		
		private static int adjustInitSegmentCount(int initSegCount, int maxSegments) {
			initSegCount = nextPowerOfTwo(initSegCount);
			if (initSegCount < MIN_INITIAL_SEGMENT_COUNT) {
				initSegCount = MIN_INITIAL_SEGMENT_COUNT; 
			} else if (initSegCount > maxSegments) {
				initSegCount = maxSegments;
			}
			return initSegCount;
		}

		static private int segmentSizeFromExpected(int expectedSize) {
			if (expectedSize < MIN_INITIAL_CAPACITY) {
				expectedSize = MIN_INITIAL_CAPACITY;
			}
			if (expectedSize > MAX_CAPACITY) {
				expectedSize = MAX_CAPACITY;
			}
			int expected = nextPowerOfTwo(expectedSize);

			int ssize = nextPowerOfTwo((int)Math.sqrt((double) expectedSize));
			if (ssize < MIN_SEGMENT_SIZE) {
				ssize = MIN_SEGMENT_SIZE;
			}
			return ssize;
		}
		
		static private int initSegmentCountFromCapacity(int segSize, int initCapacity) {
			if (initCapacity > MAX_CAPACITY) {
				initCapacity = MAX_CAPACITY;
			}
			int isegs = initCapacity / segSize;
			if (isegs < MIN_INITIAL_SEGMENT_COUNT) {
				return MIN_INITIAL_SEGMENT_COUNT;
			}
			isegs = nextPowerOfTwo(isegs);
			int maxSegs = MAX_CAPACITY / segSize;
			if (isegs > maxSegs) {
				isegs = maxSegs;
			}
			return isegs;
		}

		private int requestedSegmentSize;
		private boolean requestedSegmentSizeSet;
		
		private int requestedInitSegments;
		private boolean requestedInitSegmentsSet;
		
		private int expectedMapSize;
		private boolean expectedMapSizeSet;
		
		private int requestedInitCapacity;
		private boolean requestedInitCapacitySet;
		
		private float requestedLoadFactor;
		private boolean requestedLoadFactorSet;
				
		private MapConfigBuilder() {
			requestedSegmentSizeSet = false;
			requestedInitSegmentsSet = false;
			expectedMapSizeSet = false;
			requestedInitCapacitySet = false;
			requestedLoadFactorSet = false;
		}
		
		public MapConfigBuilder withSegmentSize(int segSize) {
			requestedSegmentSize = segSize;
			requestedSegmentSizeSet = true;
			return this;
		}
		
		public MapConfigBuilder withExpectedMapSize(int expectedSize) {
			expectedMapSize = expectedSize;
			expectedMapSizeSet = true;
			return this;
		}
		
		public MapConfigBuilder withInitCapacity(int initCap) {
			requestedInitCapacity = initCap;
			requestedInitCapacitySet = true;
			return this;
		}
		
		public MapConfigBuilder withInitSegmentCount(int segCount) {
			requestedInitSegments = segCount;
			requestedInitSegmentsSet = true;
			return this;
		}
		
		public MapConfigBuilder withLoadFactor(float factor) {
			requestedLoadFactor = factor;
			requestedLoadFactorSet = true;
			return this;
		}
		
		public MapConfig build() {
			int segmentSize;
			if (requestedSegmentSizeSet) {
				segmentSize = adjustSegmentSize(requestedSegmentSize);
			} else if (expectedMapSizeSet) {
				segmentSize = segmentSizeFromExpected(expectedMapSize);
			} else {
				segmentSize = MIN_SEGMENT_SIZE;
			}
			int maxSegments = MAX_CAPACITY / segmentSize;

			int initSegments;
			if (requestedInitSegmentsSet) {
				initSegments = adjustInitSegmentCount(requestedInitSegments, maxSegments);
			} else if (requestedInitCapacitySet) {
				initSegments = initSegmentCountFromCapacity(segmentSize, requestedInitCapacity);
			} else {
				initSegments = MIN_INITIAL_SEGMENT_COUNT;
			}
			
			float loadFactor;
			if (requestedLoadFactorSet) {
				loadFactor = adjustLoadFactor(requestedLoadFactor);
			} else {
				loadFactor = DEFAULT_LOAD_THRESHOLD;
			}
			return new MapConfig(segmentSize, initSegments, maxSegments, loadFactor);
		}
		
		
	}
	


}