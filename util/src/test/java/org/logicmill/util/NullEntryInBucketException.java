package org.logicmill.util;

import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;

/** Thrown by {@link ConExtHopsHashMapAuditor#verifyMapIntegrity(boolean, int)}
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
