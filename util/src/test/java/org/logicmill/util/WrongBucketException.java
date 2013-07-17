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

import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;

/** Thrown by {@link ConExtHopsHashMapAuditor#verifyMapIntegrity(boolean, int)}
 * to indicate the detection of an entry whose hash code does not match the 
 * bucket in which it was found.
 * @author David Curtis
 *
 */
public class WrongBucketException extends SegmentIntegrityException {
	private static final long serialVersionUID = 2425500101047242622L;
	final int bucketIndex;
	final int offset;
	final int bucketIndexFromEntry;
	
	/** Creates a new WrongBucketException with the specified details.
	 * @param segmentProbe proxy for segment in which exception occurred; uses 
	 * reflection to access private segment internals
	 * @param bucketIndex index of the bucket in which the offending entry
	 * was detected
	 * @param offset offset (from the bucket index) of the offending entry
	 * @param bucketIndexFromEntry the bucket index derived from the 
	 * entry's hash code
	 */
	public WrongBucketException(SegmentProbe segmentProbe, int bucketIndex, int offset, int bucketIndexFromEntry) {
		super(String.format("entry at buckets[%d + %d]: index from entry hash code (%d) doesn't match bucket",
				bucketIndex, offset, bucketIndexFromEntry), segmentProbe);
		this.bucketIndex = bucketIndex;
		this.offset = offset;
		this.bucketIndexFromEntry = bucketIndexFromEntry;
	}
	/** 
	 * @return index of the bucket in which the offending entry was detected
	 */
	public int getBucketIndex() { return bucketIndex; }
	/**
	 * @return offset (from bucket index) of the offending entry
	 */
	public int getOffset() { return offset; }
	/**
	 * @return the bucket index derived from the entry's hash code
	 */
	public int getBucketIndexFromEntry() { return bucketIndexFromEntry; }
}
