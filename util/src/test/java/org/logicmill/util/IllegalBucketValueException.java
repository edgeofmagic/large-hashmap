package org.logicmill.util;

import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;

/** Thrown by {@link ConExtHopsHashMapAuditor#verifyMapIntegrity(boolean, int)}
 * to indicate that an illegal value was encountered in the 
 * {@code ConExtHopsHashMap.Segment.buckets} array.
 * Legal bucket values include only {@code NULL_OFFSET} (-1) and integers 
 * between 0 (inclusive) and {@code HOP_RANGE} (32, exclusive).
 * @author David Curtis
 *
 */
public class IllegalBucketValueException extends SegmentIntegrityException {
	private static final long serialVersionUID = 7028987085817512542L;
	private final int bucketIndex;
	private final int bucketValue;
	
	/** Creates an IllegalBucketValueException with the specified details.
	 * @param segmentProbe proxy for segment in which exception occurred; uses 
	 * reflection to access private segment internals
	 * @param bucketIndex index in the {@code buckets} array where the illegal
	 * value occurred
	 * @param bucketValue illegal value observed at {@code bucketIndex}
	 */
	public IllegalBucketValueException(SegmentProbe segmentProbe, int bucketIndex, int bucketValue) {
		super(String.format("illegal value in buckets[%d]: %d", bucketIndex, bucketValue), segmentProbe);
		this.bucketIndex = bucketIndex;
		this.bucketValue = bucketValue;
	}
	/**
	 * @return index in the {@code buckets} array of the illegal value
	 */
	public int getBucketIndex() { return bucketIndex; }
	/**
	 * @return illegal value observed at {@code bucketIndex}
	 */
	public int getBucketValue() { return bucketValue; }
}
