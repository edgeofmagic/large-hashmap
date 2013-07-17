package org.logicmill.util;

import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;

/** Thrown by {@link ConExtHopsHashMapAuditor#verifyMapIntegrity(boolean, int)}
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
