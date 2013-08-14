package org.logicmill.util.concurrent;
import java.util.Arrays;
import sun.misc.Unsafe;

/**
 * A {@code byte} array in which elements behave as though
 * declared {@code volatile}.
 * @author David Curtis
 */
@SuppressWarnings("restriction")
public class VolatileByteArray {

	private static Unsafe unsafe = UnsafeAccess.getUnsafe();
	private static final int base = unsafe.arrayBaseOffset(byte[].class);
	private static final int scale = unsafe.arrayIndexScale(byte[].class);
	private final byte[] array;

	private long rawIndex(int i) {
		if (i < 0 || i >= array.length)
			throw new IndexOutOfBoundsException("index " + i);
		return base + (long) i * scale;
	}

	/**
	 * Creates a new VolatileByteArray of given length.
	 *
	 * @param length the length of the array
	 */
	public VolatileByteArray(int length) {
		array = new byte[length];
		// must perform at least one volatile write to conform to JMM
		if (length > 0)
			unsafe.putByteVolatile(array, rawIndex(0), (byte)0);
	}

	/**
	 * Creates a new VolatileByteArray with the same length as, and
	 * all elements copied from, the given array.
	 *
	 * @param array the array to copy elements from
	 * @throws NullPointerException if array is null
	 */
	public VolatileByteArray(byte[] array) {
		if (array == null)
			throw new NullPointerException();
		int length = array.length;
		if (length > 0) {
			int last = length-1;
			this.array = Arrays.copyOf(array, array.length);
			// Do the last write as volatile
			unsafe.putByteVolatile(this.array, rawIndex(last), array[last]);
		} else {
			this.array = new byte[0];
		}
	}

	/**
	 * Creates a new VolatileByteArray of given length, and
	 * set all elements with the given value.
	 * 
	 * @param length the length of the array
	 * @param value the value with which the array elements are initialized
	 */
	public VolatileByteArray(int length, byte value) {
		array = new byte[length];
		if (length > 0) {
			int last = length-1;
			Arrays.fill(array, 0, last, value);
			// Do the last write as volatile
			unsafe.putByteVolatile(this.array, rawIndex(last), array[last]);
		}

	}

	/**
	 * Returns the length of the array.
	 *
	 * @return the length of the array
	 */
	public final int length() {
		return array.length;
	}

	/**
	 * Gets the current value at position {@code i}.
	 *
	 * @param i the index
	 * @return the current value
	 */
	public final byte get(int i) {
		return unsafe.getByteVolatile(array, rawIndex(i));
	}

	/**
	 * Sets the element at position {@code i} to the given value.
	 *
	 * @param i the index
	 * @param newValue the new value
	 */
	public final void set(int i, byte newValue) {
		unsafe.putByteVolatile(array, rawIndex(i), newValue);
	}

}
