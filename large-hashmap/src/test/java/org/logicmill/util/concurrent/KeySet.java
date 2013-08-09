package org.logicmill.util.concurrent;
import org.apache.commons.math3.random.ISAACRandom;
import org.apache.commons.math3.random.BitsStreamGenerator;

/** A container for managing a set of memory-resident byte-array keys.
 * KeySet supports performance testing of hash functions; a large number
 * of readily-available keys can be supplied to a hash function under test
 * without the overhead of constructing keys, generating random numbers,
 * or reading keys from a file. KeySets are also 
 * useful for managing collections of keys with known stress-inducing 
 * properties (with respect to hash functions), where the same collection
 * of keys can be hashed by multiple functions for comparative purposes.
 * <p>A concrete sub-class must implement a constructor that initializes
 * the <code>keys</code> array and sets <code>keyCount</code>, and 
 * implement <code>getKeySizeHint</code>.  
 * @author David Curtis
 *
 */
public abstract class KeySet implements KeySource {
	
	
	/**
	 * The number of keys managed by the set. Also, the size of
	 * the first dimension of the <code>keys</code> array.
	 */
	protected int keyCount;
	
	/**
	 * Array containing keys.
	 */
	protected byte[][] keys;
	
	/**
	 * Index of the key that will be returned the next time
	 * getKey is called. 
	 */
	protected int iNextKey = 0;	
	
	/**
	 * @see org.logicmill.hash.test.KeySource#getKeySizeHint()
	 */
	@Override
	abstract public int getKeySizeHint();
	

	/**
	 * Returns the number of keys in the set.
	 * @return number of keys
	 */
	@Override
	public int getKeyCount() {
		return keyCount;
	}	
	
	/**
	 * Returns <code>true</code> if more keys are available in the set. 
	 * @return true if more keys are available
	 * @throws IllegalStateException if the set has not been initialized
	 */
	@Override
	public boolean hasMoreKeys() {
		return (iNextKey < keyCount);
	}
	
	
	/**
	 * Returns the next available key in the set. If the
	 * set is exhausted, <code>getKey</code> will return
	 * <code>null</code>.
	 * @return the next available key; null if the set is exhausted
	 * @throws IllegalStateException if the set has not been initialized
	 */
	@Override
	public byte[] getKey() {
		if (iNextKey >= keyCount) {
			return null;
		} else {
			return keys[iNextKey++];
		}			
	}
	
	private int copyToBuf(byte[] key, byte[] buf) {
		int nBytes = key.length;
		if (buf.length < key.length) {
			nBytes = buf.length;
		}
		for (int i = 0; i < nBytes; i++) {
			buf[i] = key[i];
		}
		return nBytes;
	}
	
	/** 
	 * Added for completeness; using this method would be 
	 * counter-productive in contexts that require low-overhead
	 * access to keys.
	 * @see org.logicmill.util.concurrent#getKey(byte[])
	 * 
	 */
	@Override
	public int getKey(byte[] buf) {
		if (iNextKey >= keyCount) {
			return -1;
		} else {
			return copyToBuf(keys[iNextKey++], buf);
		}			
		
	}
	
	/** Returns the <code>i<sup>th</sup></code> element in the internal key array.
	 * @param i index of the key to be returned
	 * @return the specified key
	 * @throws ArrayIndexOutOfBoundsException if {@code i < 0} or {@code i >=} 
	 * number of keys in this set
	 */
	public byte[] getKey(int i) {
		if (i < 0 || i >= keyCount) {
			throw new ArrayIndexOutOfBoundsException();
		}
		return keys[i];
	}
	
	/** Returns the number of keys in this set.
	 * @return number of keys in this set
	 */
	public int size() {
		return keyCount;
	}
	
	/**
	 * Resets to the initialized state, as if <code>getKey</code> had never 
	 * been called.
	 */
	@Override
	public void reset() {
		iNextKey = 0;
	}
	
	/**
	 * Re-orders the sequence to a pseudo-random permutation. Has utility
	 * for performance testing in some cases -- if keys are allocated
	 * from the heap as they are added, performance measurements may be
	 * skewed by caching.
	 * @param seed for the random number generator; different values result in different permutations
	 */
	public void shuffle(long seed) {
		// Fisher-Yates/Knuth shuffle
		BitsStreamGenerator rng = new ISAACRandom(seed);
		for (int i = keyCount - 1; i > 0; i--) {
			int iRnd = rng.nextInt(i+1);
			if (iRnd != i) {
				byte[] tmp = keys[i];
				keys[i] = keys[iRnd];
				keys[iRnd] = tmp;
			}
		}
	}
}
