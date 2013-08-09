package org.logicmill.util.concurrent;

/**
 * An interface for classes that provide sets of keys for testing hash 
 * algorithms. An instance of any implementation will produce a finite 
 * number of keys. This allows tests to be written with the assumption that
 * they can (in general) run until the key source is exhausted.
 * A key source implementation may or may not know at construction time
 * how many keys it will be able to provide. A programmer has the option 
 * of allowing the key source to allocate an individual buffer for each key,
 * or to reduce garbage generation by passing the source a buffer in which
 * to put a key. Key source implementations may provide a hint for
 * for buffer allocation (see {@link #getKeySizeHint()}.
 * @author David Curtis
 *
 */
public interface KeySource {

	/**
	 * Returns the next available key. If no key is available, returns null.
	 * @return next available key, or null if none is available
	 */
	public byte[] getKey();
	
	/**
	 * Deposits the next available key into the provided buffer. 
	 * If no key is available, the return value is -1, otherwise, the return
	 * value is the size of the key. If the next available key is longer than
	 * the buf array, it will be truncated to fit.
	 * @param buf on exit, contains the key
	 * @return the size of the key, or -1 if no key is available
	 */
	public int getKey(byte[] buf);
	
	/** 
	 * Returns true if more keys are available from this source. 
	 * In general, test programs using key sources should follow this
	 * pattern:
	 * <pre><code>
	 * while (source.hasMoreKeys()) {
	 * 	byte[] key = source.getKey();
	 *  	//... use key ...
	 * }
	 * </code></pre>
	 * @return true if keys are available
	 */
	public boolean hasMoreKeys();
	
	/**
	 * Suggests a reasonable buffer allocation to accommodate keys from this 
	 * source, for use with {@link #getKey(byte[])}.
	 * 
	 * @return the maximum expected size of a key from this source
	 */
	public int getKeySizeHint();
	
	/**
	 * Returns the number of keys available from this source after construction
	 * or reset. If a source cannot reasonably determine this in advance, the 
	 * return value is -1.
	 * @return number of keys available at initialization, or -1 if unknown
	 */
	public int getKeyCount();
		
	/**
	 * Resets the state of the source to the state that existed immediately 
	 * after initialization. Precise meaning may vary among source 
	 * implementations.
	 */
	public void reset();
	
}
