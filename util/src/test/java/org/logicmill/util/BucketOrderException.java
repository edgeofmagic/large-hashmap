package org.logicmill.util;

import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;

/** Thrown by {@link ConExtHopsHashMapAuditor#verifyMapIntegrity(boolean, int)} 
 * to indicate that entries in a hash map bucket are not in order. Order in the
 * bucket's linked list must correspond to the order of increasing offsets from
 * the bucket index.
 * 
 * @author David Curtis
 *
 */
public  class BucketOrderException extends SegmentIntegrityException {
	private static final long serialVersionUID = 6827529226937201607L;
	private final int bucketIndex;
	private final int offset;
	private final int nextOffset;
	
	/** Creates a BucketOrderException with the specified details.
	 * @param segmentProbe proxy for segment in which exception occurred; uses 
	 * reflection to access private segment internals
	 * @param bucketIndex index of the bucket with out-of-order entries
	 * @param offset offset from bucket index of the entry preceding the 
	 * out-of-order entry
	 * @param nextOffset offset from bucket index of the out-of-order entry
	 */
	public BucketOrderException(SegmentProbe segmentProbe, int bucketIndex, 
			int offset, int nextOffset) {
		super(String.format(
				"offsets out of order in buckets[%d + %d]: next offset %d", 
				bucketIndex, offset, nextOffset), segmentProbe);
		this.bucketIndex = bucketIndex;
		this.offset = offset;
		this.nextOffset = nextOffset;
	}
	/**
	 * @return the index of the bucket containing the out-of-order entry
	 */
	public int getBucketIndex() { return bucketIndex; }
	/**
	 * @return the offset of the entry preceding the out-of-order entry
	 */
	public int getOffset() { return offset; }
	/**
	 * @return the offset of the out-of-order entry
	 */
	public int getNextOffset() { return nextOffset; }		
}
