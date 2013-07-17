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
