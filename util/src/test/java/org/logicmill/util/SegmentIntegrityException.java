package org.logicmill.util;

import org.logicmill.util.ConExtHopsHashMapProbe.SegmentProbe;

/** An abstract base class for exceptions thrown by 
 * {@link ConExtHopsHashMapAuditor#verifyMapIntegrity(boolean, int)},
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
