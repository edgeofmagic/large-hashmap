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

import org.logicmill.util.concurrent.ConcurrentLargeHashMap;
import org.logicmill.util.concurrent.ConcurrentLargeHashMapProbe.SegmentProbe;


/** A utility associated with {@link ConcurrentLargeHashMap} that collects
 * and reports performance-related statistics about a map's internal structure,
 * focusing particularly on bucket structure. 
 * To apply this class to a map instance, construct an inspector object with
 * a reference to the map instance:
 * <pre><code>
 * ConcurrentLargeHashMap&lt;Key,Value&gt; map = new ConcurrentLargeHashMap&lt;Key,Value&gt;(1024, 8, 0.8F);
 * ConcurrentLargeHashMapInspector inspector = new ConcurrentLargeHashMapInspector(map);
 * ...
 * </code></pre>
 * 
 * @TODO finish javadoc
 * 
 * @author David Curtis
 *
 */
public class ConcurrentLargeHashMapInspector {
	
	private final ConcurrentLargeHashMapProbe mapProbe;

	public ConcurrentLargeHashMapInspector(ConcurrentLargeHashMap map) {
		mapProbe = new ConcurrentLargeHashMapProbe(map);
	}
	
	public MapBucketAssay assayMap() {
		return new MapBucketAssay(mapProbe);
	}	
	
	private class SegmentAssayIterator implements Iterator<SegmentBucketAssay> {
		
		private Iterator<SegmentProbe> segProbeIter;
		private SegmentAssayIterator() {
			segProbeIter = mapProbe.getSegmentIterator();
		}

		@Override
		public boolean hasNext() {
			return segProbeIter.hasNext();
		}

		@Override
		public SegmentBucketAssay next() {
			return new SegmentBucketAssay(segProbeIter.next());
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
	public Iterator<SegmentBucketAssay> iterator() {
		return new SegmentAssayIterator();
		
	}
}
