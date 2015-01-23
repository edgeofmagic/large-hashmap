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

import org.logicmill.util.concurrent.ConcurrentHashMapProbe.SegmentProbe;

/** Thrown by {@link ConcurrentHashMapAuditor#verifyMapIntegrity(boolean, int)}
 * to indicate that a non-null entry in the specified segment's {@code entries} 
 * array is not in the linked list of the bucket identified by the entry's hash
 * code.
 * @author David Curtis
 *
 */
public class EntryNotReachableException extends SegmentIntegrityException {
	private static final long serialVersionUID = 3786453830058665917L;
	private final int bucketIndex;
	private final int entryIndex;
	
	/** Creates an EntryNotReachableException with the specified details.
	 * @param segmentProbe proxy for segment in which exception occurred; uses 
	 * reflection to access private segment internals
	 * @param bucketIndex index of the bucket that should contain the entry
	 * @param entryIndex index of the entry that is not reachable in the
	 * linked list of the bucket at {@code bucketIndex}
	 */
	public EntryNotReachableException(SegmentProbe segmentProbe, 
			int bucketIndex, int entryIndex) {
		super(String.format("entries[%d] not reachable in buckets[%d]", 
				entryIndex, bucketIndex), segmentProbe);
		this.bucketIndex = bucketIndex;
		this.entryIndex = entryIndex;
	}
	
	/**
	 * @return index of the bucket that should contain the entry
	 */
	public int getBucketIndex() { return bucketIndex; }
	/**
	 * @return index of the entry that is not reachable in the
	 * linked list of the bucket at {@code bucketIndex}
	 */
	public int getEntryIndex() { return entryIndex; }
}
