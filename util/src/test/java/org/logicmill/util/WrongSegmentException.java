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

import org.logicmill.util.ConcurrentLargeHashMapProbe.SegmentProbe;

/** Thrown by {@link ConcurrentLargeHashMapAuditor#verifyMapIntegrity(boolean, int)}
 * to indicate the detection of an entry whose hash code does not match the 
 * shared bit pattern for the segment in which it was found.
 * @author David Curtis
 *
 */
public class WrongSegmentException extends SegmentIntegrityException { 
	private static final long serialVersionUID = 2503004732479118328L;
	final int bucketIndex;
	final int offset;
	final int sharedBitsFromEntry;
	
	/** Create a WrongSegmentException with the specified details.
	 * @param segmentProbe proxy for segment in which exception occurred; uses 
	 * reflection to access private segment internals
	 * @param bucketIndex index of the bucket in which the offending entry
	 * was detected
	 * @param offset offset (from the bucket index) of the offending entry
	 * @param sharedBitsFromEntry shared bit pattern derived from the
	 * entry's hash code
	 */
	public WrongSegmentException(SegmentProbe segmentProbe, int bucketIndex, int offset, int sharedBitsFromEntry) {
		super(String.format("entry at buckets[%d + %d]: shared bits from entry hash code (%d) doesn't match bucket",
				bucketIndex, offset, sharedBitsFromEntry), segmentProbe);
		this.bucketIndex = bucketIndex;
		this.offset = offset;
		this.sharedBitsFromEntry = sharedBitsFromEntry;
	}
	/**
	 * @return index of the bucket in which the offending entry
	 * was detected
	 */
	public int getBucketIndex() { return bucketIndex; }
	/**
	 * @return offset (from the bucket index) of the offending entry
	 */
	public int getOffset() { return offset; }
	/**
	 * @return shared bit pattern derived from the entry's hash code
	 */
	public int getSharedBitsFromEntry() { return sharedBitsFromEntry; }
}
