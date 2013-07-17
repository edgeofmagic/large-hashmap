package org.logicmill.util;

import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;

/** Thrown by {@link ConExtHopsHashMapAuditor#verifyMapIntegrity(boolean, int)}
 * to indicate that an illegal value was observed in the 
 * {@code ConExtHopsHashMap.Segment.offsets} array. Legal offset values include
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
