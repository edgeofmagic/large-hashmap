package org.logicmill.util;

import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;

/** Thrown by {@link ConExtHopsHashMapAuditor#verifyMapIntegrity(boolean, int)}
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
