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

import java.util.Iterator;

import org.logicmill.util.concurrent.ConcurrentLargeHashMapProbe.SegmentProbe;

public class MapBucketAssay extends BucketStructureAssay {
	private final ConcurrentLargeHashMapProbe mapProbe;
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
	
	public MapBucketAssay(ConcurrentLargeHashMapProbe mapProbe) {
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
