package org.logicmill.util;

import java.util.Iterator;
import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;

public class MapBucketAssay extends BucketStructureAssay {
	private final ConExtHopsHashMapProbe mapProbe;
	private final int segmentCount;
	private final int directorySize;
	
	private void incorporate(BucketStructureAssay assay) {
		addBins(this.getBucketSizeBins(), assay.getBucketSizeBins());
		addBins(this.getFirstOffsetBins(), assay.getFirstOffsetBins());
		addBins(this.getOffsetBins(), assay.getOffsetBins());
		addBins(this.getAdjacentEntryCountBins(), assay.getAdjacentEntryCountBins());
		addBins(this.getSpanSumBins(), assay.getSpanSumBins());
	}
	
	private void addBins(long[] to, long[] from) {
		int binCount = to.length;
		if (from.length < binCount) {
			binCount = from.length;
		}
		for (int i = 0; i < binCount; i++) {
			to[i] += from[i];
		}
	}
	
	public MapBucketAssay(ConExtHopsHashMapProbe mapProbe) {
		super(mapProbe.getSegmentCount()*mapProbe.getSegmentSize(), mapProbe.getHopRange());
		this.mapProbe = mapProbe;
		this.segmentCount = mapProbe.getSegmentCount();
		this.directorySize = mapProbe.getDirectorySize();
		Iterator<SegmentProbe> segIter = mapProbe.getSegmentIterator();
		int segCount = 0;
		while (segIter.hasNext()) {
			this.incorporate(new SegmentBucketAssay(segIter.next()));
			segCount++;
		}
		summarize();
	}
	public int getSegmentCount() {
		return segmentCount;
	}
	public int getDirectorySize() {
		return directorySize;
	}
}
