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

import java.util.Iterator;

import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;


/** A support class for {@link ConExtHopsHashMap} that performs integrity
 * checking and reports details of the map's internal structure. To apply
 * this class to a map instance, pass a reference to that instance to the 
 * manager's constructor:
 * <pre><code>
 * ConExtHopsHashMap&lt;Key,Value&gt; map = new ConExtHopsHashMap&lt;Key,Value&gt;(1024, 8, 0.8F);
 * ConExtHopsHashMapManager manager = new ConExtHopsHashMapManager(map);
 * ...
 * manager.integrityCheck(true, 0);
 * </code></pre>
 * This class contains
 * exceptions as nested classes. Instances of these exception classes may
 * by thrown by {@link #verifySegmentIntegrity(boolean, int)}, or may be returned
 * in a list, depending on parameter values for the invocation of 
 * {@code integrityCheck()}.<p>
 * Metrics
 * @author David Curtis
 *
 */
public class ConExtHopsHashMapInspector {
	
	private final ConExtHopsHashMapProbe mapProbe;

	public ConExtHopsHashMapInspector(ConExtHopsHashMap map) {
		mapProbe = new ConExtHopsHashMapProbe(map);
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
