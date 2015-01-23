package org.logicmill.util.concurrent;
import org.apache.commons.math3.random.ISAACRandom;


/**
 * A source of keys filled with pseudo-random values. Keys are generated
 * as requested. This class uses the ISAAC pseudo-random number generator
 * (see <a href="http://burtleburtle.net/bob/rand/isaacafa.htm">
 * http://burtleburtle.net/bob/rand/isaacafa.htm</a>).
 * @author David Curtis
 *
 */
public class RandomKeyStream implements KeySource<ByteArrayKey> {
	
	private final long seed;
	private final ISAACRandom rng;
	private final int keySize;
	private final int limit;
	private int keyCount;
	
	/**
	 * Creates a source of random keys. 
	 * @param keySize size of keys returned by {@link #getKey()}
	 * @param seed initializing value for the pseudo-random number generator
	 * @param limit the number of keys generated before <code>hasMoreKeys</code> 
	 * returns false
	 */
	public RandomKeyStream(int keySize, long seed, int limit) {
		this.seed = seed;
		rng = new ISAACRandom(seed);
		this.keySize = keySize;
		this.limit = limit;
		keyCount = 0;
	}

	/**
	 * Returns a key filled with generated pseudo-random values, or null if
	 * the limit for generated keys has been reached. The size of the returned
	 * key is the value of the keySize parameter for the constructor 
	 * {@link #RandomKeyStream(int, long, int)}.
	 * 
	 * @see org.logicmill.hash.test.KeySource#getKey()
	 */
	@Override
	public ByteArrayKey getKey() {
		if (hasMoreKeys()) {
			byte[] buf = new byte[keySize];
			rng.nextBytes(buf);
			keyCount++;
			return new ByteArrayKey(buf);
		} else {
			return null;
		}
	}

	/**
	 * {@inheritDoc}
	 * @see org.logicmill.hash.test.KeySource#hasMoreKeys()
	 */
	@Override
	public boolean hasMoreKeys() {
		return (keyCount < limit);

	}

	/**
	 * {@inheritDoc}
	 * @see org.logicmill.hash.test.KeySource#reset()
	 */
	@Override
	public void reset() {
		rng.setSeed(seed);
		keyCount = 0;
	}

	/** 
	 * Fills the provided buffer with generated pseudo-random
	 * numbers. The entire buffer is filled, regardless of 
	 * <code>keySize</code>.
	 * @see org.logicmill.hash.test.KeySource#getKey(byte[])
	 */
/*	@Override
	public int getKey(byte[] buf) {
		rng.nextBytes(buf);
		return buf.length;
	}
*/
	/**
	 * Returns the value specified in the <code>keySize</code> parameter
	 * for the constructor {@link #RandomKeyStream(int, long, int)}.
	 * @see org.logicmill.hash.test.KeySource#getKeySizeHint()
	 */
	@Override
	public int getKeySizeHint() {
		return keySize;
	}

	/** 
	 * Returns the value specified in the <code>limit</code> parameter
	 * for the constructor {@link #RandomKeyStream(int, long, int)}.
	 * @see org.logicmill.hash.test.KeySource#getKeyCount()
	 */
	@Override
	public int getKeyCount() {
		return limit;
	}

}
