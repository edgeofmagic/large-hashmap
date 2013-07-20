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

/**Thrown by {@link ConcurrentLargeHashMapAuditor#verifyMapIntegrity(boolean, int)} 
 * to indicate internal disagreement regarding the number of entries in a 
 * segment. Each segment maintains an explicit entry count, incrementing on 
 * successful put operations and decrementing on successful remove operations. 
 * The integrity check counts entries reachable in bucket lists, and non-null 
 * entry references in the {@code ConcurrentLargeHashMap.Segment.entries} array. 
 * All three counts should agree.
 * @author David Curtis
 *
 */
public class EntryCountException extends SegmentIntegrityException {
	private static final long serialVersionUID = -7286613024278086823L;
	final int entryCountInBuckets;
	final int segmentEntryCount;
	final int nonNullEntryCount;
	
	/** Creates an EntryCountException with the specified details.
	 * @param segmentProbe proxy for segment in which exception occurred; uses 
	 * reflection to access private segment internals
	 * @param entryCountInBuckets count of entries reachable in bucket lists
	 * @param segmentEntryCount count of entries maintained by put and remove 
	 * operations
	 * @param nonNullEntryCount count of non-null references in entries array
	 */
	public EntryCountException(
			SegmentProbe segmentProbe, int entryCountInBuckets, 
			int segmentEntryCount, int nonNullEntryCount) {
		super(String.format("entry count mismatch: entry count in buckets %d, "
				+"segment entry count %d, non-null entry count %d", 
				entryCountInBuckets, segmentEntryCount, nonNullEntryCount), 
				segmentProbe);
		this.entryCountInBuckets = entryCountInBuckets;
		this.segmentEntryCount = segmentEntryCount;
		this.nonNullEntryCount = nonNullEntryCount;
	}
	/**
	 * @return count of entries reachable in bucket lists
	 */
	public int getEntryCountInBuckets() { return entryCountInBuckets; }
	/**
	 * @return count of entries maintained by put and remove operations
	 */
	public int getSegmentEntryCount() { return segmentEntryCount; }
	/**
	 * @return count of non-null references in entries array
	 */
	public int getNonNullEntryCount() { return nonNullEntryCount; }
}
