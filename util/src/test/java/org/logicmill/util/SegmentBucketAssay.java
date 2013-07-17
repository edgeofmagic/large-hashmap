package org.logicmill.util;

import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;

public class SegmentBucketAssay extends BucketStructureAssay {
	
	private int sharedBits;
	private int localDepth;
	private int serialID;
	
	private boolean isBitSet(int bitIndex, long map) {
		return ((map & (1 << bitIndex)) != 0);
	}
	
	public SegmentBucketAssay(SegmentProbe segProbe) {
		super(segProbe.getMapProbe().getSegmentSize(), segProbe.getMapProbe().getHopRange());
		serialID = segProbe.getSerialID();
		sharedBits = segProbe.getSharedBits();
		localDepth = segProbe.getLocalDepth();

		for(int bucketIndex = 0; bucketIndex < getCapacity(); bucketIndex++) {
			if (!segProbe.isBucketEmpty(bucketIndex)) {
				long bucketMap = segProbe.getBucketMap(bucketIndex);
				int bucketSize = Long.bitCount(bucketMap);
				if (bucketSize > 0) {
					int firstOffset = 0;
					while (!isBitSet(firstOffset, bucketMap)) {
						firstOffset++;
					}
					int adjacentEntryCount = 0;
					getFirstOffsetBins()[firstOffset]++;
					getOffsetBins()[firstOffset]++;
					int nextOffset = firstOffset+1;
					int prevOffset = firstOffset;
					while (nextOffset < getHopRange()) {
						if (isBitSet(nextOffset, bucketMap)) {
							getOffsetBins()[nextOffset]++;
							if (nextOffset == prevOffset+1) {
								adjacentEntryCount++;
							} 
							prevOffset = nextOffset;
						}
						nextOffset++;						
					}
					getBucketSizeBins()[bucketSize]++;
					getAdjacentEntryCountBins()[bucketSize] += adjacentEntryCount;
					getSpanSumBins()[bucketSize] += (prevOffset - firstOffset);
				}
			}
		}
		summarize();
	}
}
