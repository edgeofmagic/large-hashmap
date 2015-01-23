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

import org.logicmill.util.concurrent.ConcurrentLargeHashMapProbe.SegmentProbe;

/** Thrown by {@link ConcurrentLargeHashMapAuditor#verifyMapIntegrity(boolean, int)}
 * to indicate that an illegal value was observed in the 
 * {@code ConcurrentHashMap.Segment.offsets} array. Legal offset values include
 * only {@code NULL_OFFSET} (-1) and integers between 0 (inclusive) and 
 * {@code HOP_RANGE} (32, exclusive).
 * All other values are illegal.
 * @author David Curtis
 *
 */
public class IllegalOffsetValueException extends SegmentIntegrityException {
	private static final long serialVersionUID = -8824296711479722977L;
	private final int bucketIndex;
	private final int offset;
	private final int offsetValue;
	
	/** Creates and IllegalOffsetValueException with the specified details.
	 * @param segmentProbe proxy for segment in which exception occurred; uses 
	 * reflection to access private segment internals
	 * @param bucketIndex index of the bucket in which the illegal offset was 
	 * observed
	 * @param offset offset (from the bucket index) of the illegal offset 
	 * value
	 * @param offsetValue the illegal offset value
	 */
	public IllegalOffsetValueException(SegmentProbe segmentProbe, 
			int bucketIndex, int offset, int offsetValue) {
		super(String.format("illegal offset value in buckets[%d + %d]: %d", 
				bucketIndex, offset, offsetValue), segmentProbe);
		this.bucketIndex = bucketIndex;
		this.offset = offset;
		this.offsetValue = offsetValue;
	}
	/**
	 * @return index of the bucket containing the illegal offset value
	 */
	public int getBucketIndex() { return bucketIndex; }
	/**
	 * @return the offset (from the bucket index) at which the illegal 
	 * value was observed
	 */
	public int getOffset() { return offset; }
	/**
	 * @return the observed illegal offset value
	 */
	public int getOffsetValue() { return offsetValue; }
}
