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

import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;


/** A reflection-based test probe for inspecting the internal data structures 
 * of an instance of ConcurrentLargeHashMap. The bulk of the workload is delegated
 * to the nested class {@code SegmentProbe}.
 * 
 * @author David Curtis
 *
 */
public class ConcurrentLargeHashMapProbe {
	
	/** A wrapper for exceptions thrown within the probe implementation.
	 * The possible causal exceptions include {@code SecurityException}, 
	 * {@code IllegalArgumentException}, {@code NoSuchFieldException}, and 
	 * {@code IllegalAccessException}. 
	 * @author David Curtis
	 *
	 */
	public static class ProbeInternalException extends RuntimeException {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2330311792122205627L;
		ProbeInternalException(String msg, Throwable cause) {
			super(msg, cause);
		}
		/** Creates a new ProbeInternalException with the specified cause.
		 * @param cause exception causing the ProbeInternalException.
		 */
		public ProbeInternalException(Throwable cause) {
			super(cause);
		}
	}
	
	/** A reflection-based probe for inspecting the internal data structures of
	 * an instance of {@code ConcurrentLargeHashMap.Segment}.
	 * @author David Curtis
	 *
	 */
	public class SegmentProbe {

		private final Object segment;
		private final AtomicIntegerArray buckets;
		private final AtomicIntegerArray offsets;
		private final AtomicReferenceArray<LargeHashMap.Entry<?,?>> entries;
		private final AtomicIntegerArray timeStamps;
		private final int indexMask;
		private final int localDepth;
		private final int sharedBits;
		private final int serialID;
		private final long sharedBitsMask;
		private final ReentrantLock segmentLock;
		
		@SuppressWarnings("unchecked")
		private SegmentProbe(Object segment) throws ProbeInternalException {
			this.segment = segment;

			try {
				buckets = ReflectionProbe.getAtomicIntegerArrayField(segment, "buckets");
				offsets = ReflectionProbe.getAtomicIntegerArrayField(segment, "offsets");
				timeStamps = ReflectionProbe.getAtomicIntegerArrayField(segment, "timeStamps");
				entries = ReflectionProbe.getAtomicReferenceArrayField(segment, "entries");
				indexMask = ReflectionProbe.getIntField(segment, "indexMask");
				localDepth = ReflectionProbe.getIntField(segment, "localDepth");
				sharedBits = ReflectionProbe.getIntField(segment, "sharedBits");
				serialID = ReflectionProbe.getIntField(segment, "serialID");
				sharedBitsMask = ReflectionProbe.getLongField(segment, "sharedBitsMask");
				segmentLock = (ReentrantLock)ReflectionProbe.getObjectField(segment, "lock");
			} catch (Exception e) {
				throw new ProbeInternalException(e);
			} 

		}
		
		private SegmentProbe(AtomicReferenceArray<?> dirArray, int segmentIndex) throws ProbeInternalException {
			this(dirArray.get(segmentIndex));
		}
		
		/** Returns the {@code buckets} array for the segment. A <i>bucket</i>
		 * is a linked list of entries, such that the hash codes for all 
		 * entries in the bucket resolve to the same bucket array index.
		 * More specifically, {@code buckets[i]} is the root of the linked list
		 * containing all entries such that <pre><code>
		 * bucketIndex(entry.getHashCode()) == i
		 * </code></pre>
		 * where {@code bucketIndex(hashCode)} returns a value derived from a 
		 * hash code that can be used as in index for the buckets array.
		 * The value of {@code buckets[i]} is an offset (from i) of the first 
		 * entry in the bucket's list.  Or,<pre><code>
		 * entries[(i + buckets[i]) % buckets.length]
		 * </code></pre>
		 * is the first entry in the bucket rooted at {@code buckets[i]}.
		 * Subsequent entries in the bucket (if any) are linked through
		 * the <i>offsets</i> array (see {@link #getOffsets()}).
		 * 
		 * @return reference to the segment's {@code buckets} array
		 * @see #getOffsets()
		 * @see #getEntries()
		 */
		public AtomicIntegerArray getBuckets() {
			return buckets;
		}
		
		/** Returns the {@code offsets} array for the segment. In conjunction
		 * with the {@code buckets} array (see {@link #getBuckets()}), 
		 * {@code offsets} describes links in the bucket linked-list
		 * structure. Given a bucket rooted at {@code buckets[i]}, the first
		 * entry is located at<pre><code>
		 * entries[i<sub>0</sub>] : i<sub>0</sub> = (i + buckets[i]) % buckets.length
		 * </code></pre>
		 * <code>offsets[i<sub>0</sub>]</code> contains either {@code NULL_OFFSET}, indicating the
		 * end of the list, or a link (as an offset from i) to the 
		 * second entry in the bucket at<pre><code>
		 * entries[i<sub>1</sub>] : i<sub>1</sub> = (i + offsets[i<sub>0</sub>]) % buckets.length
		 * </code></pre>
		 * and subsesquent entries are at<pre><code>
		 * entries[i<sub>j</sub>] : i<sub>j</sub> = (i + offsets[i<sub>j-1</sub>]) % buckets.length
		 * </code></pre>
		 * until the list is terminated by a {@code NULL_OFFSET} value (-1) in <code>offsets[i<sub>j</sub>]</code>.
		 * @return reference to the segment's {@code offsets} array
		 */
		public AtomicIntegerArray getOffsets() {
			return offsets;
		}
		
		/** Returns the {@code timeStamps} array for the segment. Time stamp
		 * values are used by retrieval operations ({@code get} and iterator 
		 * operations) to detect concurrent bucket updates that may invalidate
		 * the retrieval results. Each element {@code timeStamps[i]} corresponds
		 * the bucket at the same index, {@code buckets[i]}. Update operations
		 * that alter bucket structure increment the bucket's time stamp at
		 * critical serialization points. Retrieval operations compare
		 * time stamp values before and after traversing a bucket, and re-try
		 * the operation if the time stamp changed during traversal.
		 * @return reference to the segment's {@code timeStamps} array
		 */
		public AtomicIntegerArray getTimeStamps() {
			return timeStamps;
		}
		
		/** Returns the {@code entries} array for the segment.
		 * @return reference to the segment's {@code entries} array
		 */
		public AtomicReferenceArray<LargeHashMap.Entry<?,?>> getEntries() {
			return entries;
		}
		
		/** Returns the segment's indexMask value. The indexMask value is used
		 * to perform the equivalent of a modulus operation on hash codes, with
		 * the divisor {@code segmentSize} (which is the length of the various
		 * arrays in the segment -- {@code buckets}, {@code entries}, and so
		 * on. {@code indexMask = segmentSize - 1}, where {@code segmentSize}
		 * is an integral power of 2. The modulus operation devolves to a
		 * bitwise AND operation.
		 * @return segment's indexMask value
		 */
		public int getIndexMask() {
			return indexMask;
		}
		
		/** Returns the <i>local depth</i> of the segment. The value of 
		 * localDepth corresponds the number of bits in hash code values
		 * (at the least-significant end of the hash code} that are common
		 * for all entries in the segment. These bits are discarded when
		 * deriving a bucket index from a hash code value. 
		 * Specifically,<pre><code>
		 * int bucketIndex(long hashCode) { return (hashCode >>> localDepth) & indexMask; }
		 * </code></pre>
		 * @return the local depth of the segment
		 * @see #getIndexMask()
		 */
		public int getLocalDepth() {
			return localDepth;
		}
		
		/** The shared bit pattern for all entries in the segment. The least significant
		 * localDepth bits for all entries in the segment should have the same value, that
		 * being {@code sharedBits}.
		 * @return the shared bit pattern for the segment
		 * @see #getLocalDepth()
		 */
		public int getSharedBits() {
			return sharedBits;
		}
		
		/** Returns the serial ID number of the segment. Each segment has a
		 * unique serial ID number. The extendible hashing algorithm allows
		 * that multiple directory indices may refer to the same segment.
		 * Segment serial ID numbers provide a means for programs traversing 
		 * the directory to determine whether a segment has already been seen 
		 * or not.
		 * @return the serial ID number of the segment
		 */
		public int getSerialID() {
			return serialID;
		}
		
		/** Returns a mask corresponding the bits in a hash code that contain
		 * the shared bit pattern for the segment. Specifically,<pre><code>
		 * 	long sharedBitsMask = (1L << localDepth) - 1;
		 * </code></pre>and for all entries in the segment,<pre><code>
		 * 	(entry.getHashCode() & getSharedBitsMask()) == getSharedBits()
		 * </code></pre>
		 * @return the shared bits mask value for the segment
		 * @see #getSharedBitsMask()
		 * @see #getLocalDepth()
		 */
		public long getSharedBitsMask() {
			return sharedBitsMask;
		}
		
		/** Returns the mutual update exclusion lock for the segment. Certain
		 * validation operations may require excluding updates from a segment.
		 * Should be used with care.
		 * @return update lock for segment
		 */
		public ReentrantLock getSegmentLock() {
			return segmentLock;
		}
		
		/** Returns the number of entries in the segment.
		 * @return number of entries in the segment
		 * @throws ProbeInternalException
		 */
		public int getEntryCount() throws ProbeInternalException {
			try {
				return ReflectionProbe.getIntField(segment, "entryCount");
			} catch (SecurityException e) {
				throw new ProbeInternalException(e);
			} catch (IllegalArgumentException e) {
				throw new ProbeInternalException(e);
			} catch (NoSuchFieldException e) {
				throw new ProbeInternalException(e);
			} catch (IllegalAccessException e) {
				throw new ProbeInternalException(e);
			}
		}
		
		
		/** Returns a calculated index modulus the segment array size
		 * @param unwrappedIndex the calculated index
		 * @return index modulus segment size
		 */
		public int wrapIndex(int unwrappedIndex) {
			return unwrappedIndex & indexMask;
		}
		
		/** Returns the bucket index derived from a hash code value:<pre><code>
		 * return (hashCode >>> getLocalDepth()) & getIndexMask();
		 * </code></pre>
		 * @param hashCode hash code value used to derive bucket index
		 * @return the index of bucket (in the {@code buckets} array 
		 * corresponding to {@code hashCode}
		 */
		public int bucketIndex(long hashCode) {
			return (int) ((hashCode >>> localDepth) & indexMask);
		}
		
		/** Returns true if the bucket at {@code bucketIndex} is empty, that 
		 * is, if {@code buckets[bucketIndex] == NULL_OFFSET}.
		 * @param bucketIndex
		 * @return {@code true} if the bucket at {@code bucketIndex} is empty
		 */
		public boolean isBucketEmpty(int bucketIndex) {
			return buckets.get(bucketIndex) == NULL_OFFSET;
		}
		
		
		@SuppressWarnings("rawtypes")
		int getBucketIndexForEntryAt(int entryIndex) {
			return bucketIndex(getEntryHashCode((LargeHashMap.Entry)entries.get(entryIndex)));
		}
		
		/** Returns the hash code for the specified entry.
		 * @param entry entry of which the hash code is returned
		 * @return hash code for {@code entry}
		 */
		public long getEntryHashCode(LargeHashMap.Entry<?,?> entry) {
			try {
				return ReflectionProbe.getLongField(entry, "hashCode");
			} catch (SecurityException e) {
				throw new ProbeInternalException(e);
			} catch (IllegalArgumentException e) {
				throw new ProbeInternalException(e);
			} catch (NoSuchFieldException e) {
				throw new ProbeInternalException(e);
			} catch (IllegalAccessException e) {
				throw new ProbeInternalException(e);
			}
		}
		
		/** Returns the map probe object to which this segment probe belongs.
		 * @return reference to the map probe object for this segment probe
		 */
		public ConcurrentLargeHashMapProbe getMapProbe() {
			return ConcurrentLargeHashMapProbe.this;
		}
		
		/** Returns a long value containing set bits that correspond to entries
		 * in the specified bucket, where the bit position corresponds to the
		 * offset of the entry from the bucket index. Specifically, if
		 * {@code ((result & (1 << n)) != 0)} then {@code buckets[bucketIndex]}
		 * contains an entry at offset {@code n}. Valid bits are 
		 * <code>2<sup>n</sup></code> where for {@code n} in {@code (1 .. 
		 * HOP_RANGE)}. HOP_RANGE cannot exceed 64 without re-designing this
		 * method, but that seems unlikely.
		 * @param bucketIndex index of the bucket whose map is returned
		 * @return the bitwise map of entries in the specified bucket
		 */
		public long getBucketMap(int bucketIndex) {
			assert bucketIndex >= 0 && bucketIndex < buckets.length();
			long bucketMap = 0;
			int oldTimeStamp, newTimeStamp = 0;
			retry:
			do {
				bucketMap = 0;
				oldTimeStamp = timeStamps.get(bucketIndex);
				int nextOffset = buckets.get(bucketIndex);
				if (nextOffset == NULL_OFFSET) {
					return 0L;
				}
				while (nextOffset != NULL_OFFSET) {
					int nextIndex = wrapIndex(bucketIndex + nextOffset);
					@SuppressWarnings("rawtypes")
					LargeHashMap.Entry entry = (LargeHashMap.Entry)entries.get(nextIndex);
					if (entry == null) {
						/*
						 * Concurrent update; try again.
						 */
						continue retry;
					}
					if (bucketIndex(getEntryHashCode(entry)) != bucketIndex) {
						/*
						 * Concurrent update; try again.
						 */
						continue retry;
					} else {
						bucketMap |= 1L << nextOffset;
					}
					nextOffset = offsets.get(nextIndex);
				}
				newTimeStamp = timeStamps.get(bucketIndex);
			} while (newTimeStamp != oldTimeStamp);
			return bucketMap;
		}
		
		/** Returns true if the bucket at {@code bucketIndex} contains the 
		 * specified entry. This method traverses the bucket linked list
		 * to determine the result, as opposed to simply comparing
		 * {@code bucketIndex(getEntryHashCode(entry))} with {@code 
		 * bucketIndex}.
		 * @param bucketIndex index of the bucket in which {@code entry} may
		 * be present
		 * @param entry entry whose presence in the bucket is to be tested
		 * @return true if the specified entry is in the specified bucket
		 */
		public boolean bucketContainsEntry(int bucketIndex, LargeHashMap.Entry<?,?> entry) {
			int offset = buckets.get(bucketIndex);
			while (offset != NULL_OFFSET) {
				LargeHashMap.Entry<?,?> bucketEntry = (LargeHashMap.Entry<?,?>)entries.get(wrapIndex(bucketIndex+offset));
				if (entry == bucketEntry) {
					return true;
				}
				int nextOffset = offsets.get(offset);
				if (nextOffset != NULL_OFFSET) {
					if (nextOffset < 0 || nextOffset >= HOP_RANGE || nextOffset < offset) {
						return false;
					}
				}
				offset = nextOffset;
			}
			return false;
		}

	}
	
	
	/** Returns a segment probe associated with the segment at the specified
	 * directory index. Note that multiple directory indices may refer to the
	 * same segment object. The result of {@code SegmentProbe.getSerialID()}
	 * may be used to avoid redundant examinations of the same segment.
	 * @param segmentIndex index in the directory of the segment whose probe is returned
	 * @return probe object associated with the specified segment
	 */
	public SegmentProbe getSegmentProbe(int segmentIndex) {
		@SuppressWarnings("rawtypes")
		AtomicReferenceArray dir = getDirectoryArray();
		Object seg = dir.get(segmentIndex);
		return new SegmentProbe(seg);
	}
	
	/** An iterator over segments in the map associated with this probe object.
	 * The iterator guarantees that each segment is returned at most once, and that 
	 * the set of segments returned by the iterator will not contain overlapping 
	 * entry sets. Consider the following situation: A map directory has 4 elements,
	 * with the following mapping to segments A, B and C.
	 * <pre>
	 * directory[0] = A
	 * directory[1] = B
	 * directory[2] = A
	 * dierctory[3] = C
	 *  </pre>
	 * A segment iterator is created, 
	 * and next() is called twice, returning A and B. Concurrently, an insertion
	 * into A causes it to be split into A0 and A1. Now the directory looks like
	 * this:
	 * <pre>
	 * directory[0] = A0
	 * directory[1] = B
	 * directory[2] = A1
	 * directory[3] = C
	 * </pre>
	 * such that the contents of A (already seen through the segment iterator)
	 * were split between A0 and A1. If the iterator returns A1, some elements in A1
	 * will be seen twice. This iterator implementation avoids this problem by
	 * marking (in a bit map) all indices occupied by any segment reference 
	 * returned by next() on the iterator.
	 * @author David Curtis
	 *
	 */
	public class SegmentIterator implements Iterator<SegmentProbe>{
		
		@SuppressWarnings("rawtypes")
		private final AtomicReferenceArray dir;
		private final int dirSize;

		private SegmentProbe nextSegment;
		int nextSegmentIndex;
		BitSet markedSegments;

		private SegmentIterator() throws ProbeInternalException {
			try {
				dir = getDirectoryArray();
			} catch (Exception e) {
				throw new ProbeInternalException(e);
			} 
			dirSize = dir.length();
			markedSegments = new BitSet(dirSize);
			nextSegmentIndex = 0;
			nextSegment = getSegment();			
		}
		
		private SegmentProbe getSegment() throws ProbeInternalException  {
			while (nextSegmentIndex < dirSize) {
				if (!markedSegments.get(nextSegmentIndex)) {
					SegmentProbe segProbe = new SegmentProbe(dir, nextSegmentIndex);
					/*
					 * mark directory[i] for i = (nextSegmentIndex + n * 2^localDepth)
					 * for n = 0 ... log2(dirSize) - 1
					 */
					int step = 1 << segProbe.getLocalDepth();
					for (int i = segProbe.getSharedBits(); i < dirSize; i += step) {
						markedSegments.set(i);
					}
					nextSegmentIndex++;
					return segProbe;
				} else {
					nextSegmentIndex++;
				}
			}
			return null;
		}


		/** Returns true if more SegmentProbe object are available.
		 * @return true if iterator is not exhausted
		 */
		@Override
		public boolean hasNext() {
			return (nextSegment != null);
		}

		/** Returns a SegmentProbe associated with the next unique segment in the map directory
		 * @return a SegmentProbe associated with next unique segment in the map directory
		 * @throws ProbeInternalException if the probe encounters any exception during this operation
		 */
		@Override
		public SegmentProbe next() throws ProbeInternalException {
			if (nextSegment != null) {
				SegmentProbe current = nextSegment;
				nextSegment = getSegment();
				return current;
			} else {
				return null;
			}
		}

		/**
		 * This operation is not supported.
		 * @throws UnsupportedOperationException unconditionally
		 */
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	

	private final Object map;
	private final int HOP_RANGE;
	private final int NULL_OFFSET;
	private final boolean GATHER_METRICS;
	private final AtomicReference<AtomicReferenceArray<?>> directory;
	private final int segmentSize;
	private final ReentrantLock dirLock;
	private final AtomicInteger forcedSplitCount;
	private final AtomicInteger thresholdSplitCount;
	
	
	@SuppressWarnings({ "unchecked"})
	ConcurrentLargeHashMapProbe(Object map) throws ProbeInternalException {
		this.map = map;
		try {
			NULL_OFFSET = ReflectionProbe.getIntField(map, "NULL_OFFSET");
			HOP_RANGE = ReflectionProbe.getIntField(map, "HOP_RANGE");
			GATHER_METRICS = ReflectionProbe.getBooleanField(map, "GATHER_EVENT_DATA");
			directory = (AtomicReference<AtomicReferenceArray<?>>) ReflectionProbe.getAtomicReferenceField(map, "directory");
			segmentSize = ReflectionProbe.getIntField(map, "segmentSize");
			dirLock = (ReentrantLock)ReflectionProbe.getObjectField(map, "dirLock");
			forcedSplitCount = ReflectionProbe.getAtomicIntegerField(map, "forcedSplitCount");
			thresholdSplitCount = ReflectionProbe.getAtomicIntegerField(map, "thresholdSplitCount");
		} catch (SecurityException e) {
			throw new ProbeInternalException(e);		
		} catch (IllegalArgumentException e) {
			throw new ProbeInternalException(e);		
		} catch (NoSuchFieldException e) {
			throw new ProbeInternalException(e);		
		} catch (IllegalAccessException e) {
			throw new ProbeInternalException(e);		
		}
		
	}	
	
	/** Returns the value of the HOP_RANGE constant, which is the maximum offset
	 * from a bucket index to any entry in that bucket.
	 * @return value of the HOP_RAMGE constant
	 */
	public int getHopRange() { return HOP_RANGE; }
	
	/** Returns the value used to indicate a null offset in the {@offsets}
	 * array and the {@code buckets} array in a segment.
	 * @return value of the NULL_OFFSET constant
	 */
	public int getNullOffset() { return NULL_OFFSET; }
	
	/** Returns the value of the GATHER_METRICS compile-time flag for the
	 * associated map. If {@code true} at compile time, the map collects
	 * data about run-time events that occur during map operations.
	 * @return the value of the GATHER_METRICS flag
	 */
	public boolean getGatherMetrics() { return GATHER_METRICS; }
	
	/** Returns the size of the directory. Note that subsequent/concurrent map 
	 * operations my increase the directory size.
	 * @return size of the directory array
	 */
	public int getDirectorySize() { return directory.get().length(); }

	/** Returns the size of segment arrays ({@code buckets}, {@code offsets},
	 * {@code entries} and {@code timeStamps}) in the map associated with this 
	 * probe object.
	 * @return the size of segment arrays in the associated map
	 */
	public int getSegmentSize() { return segmentSize; }
	
	/** Returns the number of unique segments currently contained in the
	 * associated map.
	 * @return number of unique segments currently contained in the
	 * associated map
	 * @throws ProbeInternalException if an exception is thrown during this 
	 * operation
	 */
	public int getSegmentCount() throws ProbeInternalException { 
		try {
			return ReflectionProbe.getIntField(map, "segmentCount");
		} catch (SecurityException e) {
			throw new ProbeInternalException(e);
		} catch (IllegalArgumentException e) {
			throw new ProbeInternalException(e);
		} catch (NoSuchFieldException e) {
			throw new ProbeInternalException(e);
		} catch (IllegalAccessException e) {
			throw new ProbeInternalException(e);
		} 
	}
	
	/**
	 * Locks the directory, causing updates that would alter the directory 
	 * structure due to segment splitting to block until 
	 * {@link #unlockDirectory()} is called by the thread owning this lock.
	 */
	public void lockDirectory() { dirLock.lock(); }
	
	/**
	 * Unlocks the directory, allowing other (potentially blocked) threads
	 * to acquire the directory lock for updates. May only be called by
	 * a thread owning the lock.
	 */
	public void unlockDirectory() { dirLock.unlock(); }
	
	/** Returns the number of occurrences of a forced split, where a segment
	 * is forced to split by an overflow exception during a {@code put} 
	 * operation. An overflow exception is thrown when a free entry slot 
	 * cannot be located within a pre-determined range limit, or when
	 * the hopscotch algorithm cannot successfully move the free entry
	 * slot to within HOP_RANGE of the target bucket index.
	 * @return number of forced split events that have occurred since the 
	 * associated map was constructed
	 */
	public int getForcedSplitCount() { return forcedSplitCount.get(); }
	
	/** Returns the number of times a segment was split because the load factor
	 * threshold would have been exceeded by a {@code put} operation.
	 * @return number of segment splits due to load threshold
	 */
	public int getThresholdSplitCount() { return thresholdSplitCount.get(); }		

	/** Returns a reference to the directory array object for the associated 
	 * map. If updates that cause segment splits (without forcing directory
	 * expansion) occur after a directory reference is obtained by this method, 
	 * the referent directory array will reflect those updates. If an update 
	 * occurs that causes directory expansion, the previously obtained
	 * directory array will not reflect that update or any subsequent updates
	 * that modify directory contents.
	 * @return reference to the directory array of the map associated with
	 * this probe
	 */
	public AtomicReferenceArray<?> getDirectoryArray() { return directory.get(); }
	
	/** Returns an iterator over segments in the map associated with this map probe.
	 * @return iterator over segments in the map associated with this probe
	 */
	public Iterator<SegmentProbe> getSegmentIterator() {
		return new SegmentIterator();
	}
	
}
