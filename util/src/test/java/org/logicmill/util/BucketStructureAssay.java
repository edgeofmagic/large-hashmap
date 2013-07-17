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
package org.logicmill.util;

abstract public class BucketStructureAssay {
	
	private final int hopRange;
	private final long capacity;
	private long entryCount;
	private long bucketCount;
	private double loadFactor;
	private final long[] bucketSizeBins;
	private final long[] offsetBins;
	private final long[] firstOffsetBins;
	private final long[] adjacentEntryCountBins;
	private final double[] adjacencyFactorBins;
	private final long[] spanSumBins;
	private final double[] spanFactorBins;
	private int maxBucketSize;
	private int maxOffset;
	private int maxFirstOffset;
	private double avgBucketSize;
	private double avgOffset;
	private double avgFirstOffset;
	private double avgAdjacencyFactor;
	private double avgSpanFactor;

	protected BucketStructureAssay(long capacity, int hopRange) {
		this.hopRange = hopRange;
		this.capacity = capacity;
		bucketSizeBins = new long[hopRange+1];
		offsetBins = new long[hopRange];
		firstOffsetBins = new long[hopRange];
		adjacentEntryCountBins = new long[hopRange+1];
		adjacencyFactorBins = new double[hopRange+1];
		spanSumBins = new long[hopRange+1];
		spanFactorBins = new double[hopRange+1];
	}

	public void summarize() {
		this.entryCount = countFromBins(offsetBins);
		this.bucketCount = countFromBins(bucketSizeBins);
		this.loadFactor = (double)entryCount/(double)capacity;

		maxBucketSize = maxFromBins(bucketSizeBins);
		maxFirstOffset = maxFromBins(firstOffsetBins);
		maxOffset = maxFromBins(offsetBins);
		
		avgBucketSize = averageFromBins(bucketSizeBins);
		avgOffset = averageFromBins(offsetBins);
		avgFirstOffset = averageFromBins(firstOffsetBins);
		
		for (int i = 2; i < adjacentEntryCountBins.length; i++) {
			if (bucketSizeBins[i] > 0) {
				adjacencyFactorBins[i] = ((double)adjacentEntryCountBins[i] / (double)bucketSizeBins[i]) / (double)(i-1);
			} else {
				adjacencyFactorBins[i] = 0.0D;
			}
		}
		double weightedAdjacencyFactorSum = 0.0D;
		for (int i = 2; i < bucketSizeBins.length; i++) {
			if (bucketSizeBins[i] > 0) {
				weightedAdjacencyFactorSum += adjacencyFactorBins[i] * bucketSizeBins[i];
			}
		}
		long bucketWithAdjacencyCount = bucketCount - bucketSizeBins[1];
		if (bucketWithAdjacencyCount > 0) {
			avgAdjacencyFactor = weightedAdjacencyFactorSum / (double)bucketWithAdjacencyCount;
		} else {
			avgAdjacencyFactor = 0.0D;
		}
		
		for (int i = 2; i < spanSumBins.length; i++) {
			if (bucketSizeBins[i] > 0) {
				spanFactorBins[i] = ((double)spanSumBins[i] / (double)bucketSizeBins[i]) / (double)(i-1);
			} else {
				spanFactorBins[i] = 0.0D;
			}
		}
		double weightedSpanFactorSum = 0.0D;
		for (int i = 2; i < bucketSizeBins.length; i++) {
			if (bucketSizeBins[i] > 0) {
				weightedSpanFactorSum += spanFactorBins[i] * bucketSizeBins[i];
			}
		}
		long spanBucketCount = bucketCount - bucketSizeBins[1];
		if (spanBucketCount > 0) {
			avgSpanFactor = weightedSpanFactorSum / (double)spanBucketCount;
		} else {
			avgSpanFactor = 0.0D;
		}


	}
	
	private long countFromBins(long[] bins) {
		long count = 0L;
		for (int i = 0; i < bins.length; i++) {
			count += bins[i];
		}
		return count;
	}
	
	private double averageFromBins(long[] bins) {
		long sum = 0;
		long count = 0;
		for (int i = 0; i < bins.length; i++) {
			sum += i * bins[i];
			count += bins[i];
		}
		return (double)sum/(double)count;
	}
	
	private int maxFromBins(long[] bins) {
		int max = 0;
		for (int i = 0; i < bins.length; i++) {
			if (bins[i] > 0L) {
				max = i;
			}
		}
		return max;
	}

	public long getCapacity() {
		return capacity;
	}
	
	public int getHopRange() {
		return hopRange;
	}
	
	public long getEntryCount() {
		return entryCount;
	}
	
	public long getBucketCount() {
		return bucketCount;
	}
	
	public double getLoadFactor() {
		return loadFactor;
	}
	
	public long[] getBucketSizeBins() {
		return bucketSizeBins;
	}
	
	public int getMaxBucketSize() {
		return maxBucketSize;
	}
	
	public double getAvgBucketSize() {
		return avgBucketSize;
	}
	
	public long[] getOffsetBins() {
		return offsetBins;
	}
	
	public int getMaxOffset() {
		return maxOffset;
	}
	
	public double getAvgOffset() {
		return avgOffset;
	}
	
	public long[] getFirstOffsetBins() {
		return firstOffsetBins;
	}
	
	public int getMaxFirstOffset() {
		return maxFirstOffset;
	}
	
	public double getAvgFirstOffset() {
		return avgFirstOffset;
	}
			
	public double[] getAdjacencyFactorBins() {
		return adjacencyFactorBins;
	}
	
	public long[] getAdjacentEntryCountBins() {
		return adjacentEntryCountBins;
	}
	
	public double getAvgAdjacencyFactor() {
		return avgAdjacencyFactor;
	}	
	
	public long[] getSpanSumBins() {
		return spanSumBins;
	}
	
	public double[] getSpanFactorBins() {
		return spanFactorBins;
	}
	
	public double getAvgSpanFactor() {
		return avgSpanFactor;
	}
}
