package org.logicmill.util.concurrent;

/**
 * A source of pre-generated keys filled with pseudo-random numbers. This
 * class uses {@link RandomKeyStream} during construction to create the
 * keys.
 * @author David Curtis
 * @see RandomKeyStream
 *
 */
public class RandomKeySet extends KeySet<ByteArrayKey> {

	private final int keySize;
	/**
	 * Creates a set of keys filled with pseudo-random numbers.
	 * @param count number of keys generated
	 * @param keySize size of the generated keys
	 * @param seed initializing value for the generator
	 */
	public RandomKeySet(int count, int keySize, long seed) {
		this.keySize = keySize;
		keys = new ByteArrayKey[count];
		keyCount = count;
		RandomKeyStream rkg = new RandomKeyStream(keySize, seed, count);
		int iKey = 0;
		while (rkg.hasMoreKeys()) {
			keys[iKey++] = rkg.getKey();
		}
	}

	/**
	 * Returns the size of the generated keys.
	 * @see org.logicmill.hash.test.KeySet#getKeySizeHint()
	 */
	@Override
	public int getKeySizeHint() {
		return keySize;
	}

}
