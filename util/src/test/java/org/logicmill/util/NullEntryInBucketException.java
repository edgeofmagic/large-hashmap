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
 * indicate that a bucket contained a null entry reference.
 * @author David Curtis
 *
 */
public class NullEntryInBucketException extends SegmentIntegrityException {
	private static final long serialVersionUID = -3810011612702169814L;
	private final int bucketIndex;
	private final int offset;
	
	/** Creates a NullEntryInBucketException with the specified details.
	 * @param segmentProbe proxy for segment in which exception occurred; uses
	 * reflection to access private segment internals
	 * @param bucketIndex index of the bucket in which the null entry was 
	 * observed
	 * @param offset offset (from bucket index) of the null entry
	 */
	public NullEntryInBucketException(SegmentProbe segmentProbe, 
			int bucketIndex, int offset) {
		super(String.format("null entry in buckets[%d + %d]", 
				bucketIndex, offset), segmentProbe);
		this.bucketIndex = bucketIndex;
		this.offset = offset;
	}
	
	/**
	 * @return index of the bucket containing the null entry
	 */
	public int getBucketIndex() { return bucketIndex; }
	
	/**
	 * @return offset (from bucket index) of the null entry
	 */
	public int getOffset() { return offset; }
	
}
