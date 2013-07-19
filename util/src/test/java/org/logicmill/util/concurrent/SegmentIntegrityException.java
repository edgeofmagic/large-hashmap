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

import org.logicmill.util.concurrent.ConcurrentLargeHashMapProbe.SegmentProbe;

/** An abstract base class for exceptions thrown by 
 * {@link ConcurrentLargeHashMapAuditor#verifyMapIntegrity(boolean, int)},
 * to indicate detection of a structural anomaly. SegmentIntegrityException 
 * encapsulates identification of the segment in which the anomaly was observed
 * (common to all subclasses).
 * @author David Curtis
 *
 */
public abstract class SegmentIntegrityException extends Exception {
	private static final long serialVersionUID = 2347232456371588074L;
	private final int serialID;
	private final int sharedBits;
	private final int localDepth;
	
	/** Creates a SegmentIntegrityException identifying the segment in which
	 * the exception occurred.
	 * @param msg the detail message
	 * @param segmentProbe proxy for segment in which exception occurred; uses 
	 * reflection to access private segment internals
	 */
	protected SegmentIntegrityException(String msg, SegmentProbe segmentProbe) {
		super(String.format("in segmentID %d[%d,%d] ", segmentProbe.getSerialID(),
				segmentProbe.getSharedBits(), segmentProbe.getLocalDepth()) + msg);
		serialID = segmentProbe.getSerialID();
		sharedBits = segmentProbe.getSharedBits();
		localDepth = segmentProbe.getLocalDepth();
	}
	
	/**
	 * @return the serial identifier of the segment
	 */
	public int getSerialID() { return serialID; }
	
	/**
	 * @return the shared bit pattern of the segment
	 */
	public int getSharedBits() { return sharedBits; }
	/**
	 * @return the local depth of the segment
	 */
	public int getLocalDepth() { return localDepth; }
}
