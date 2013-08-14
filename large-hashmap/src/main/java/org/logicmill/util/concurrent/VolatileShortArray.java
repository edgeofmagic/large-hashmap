package org.logicmill.util.concurrent;
import java.util.Arrays;

import sun.misc.Unsafe;

/**
 * A {@code short} array in which elements behave as though
 * declared {@code volatile}.
 * @author David Curtis
 */
@SuppressWarnings("restriction")
public class VolatileShortArray {

    private static final Unsafe unsafe = UnsafeAccess.getUnsafe();
    private static final int base = unsafe.arrayBaseOffset(short[].class);
    private static final int scale = unsafe.arrayIndexScale(short[].class);
    private final short[] array;

    private long rawIndex(int i) {
        if (i < 0 || i >= array.length)
            throw new IndexOutOfBoundsException("index " + i);
        return base + (long) i * scale;
    }

    /**
     * Creates a new VolatileShortArray of given length.
     *
     * @param length the length of the array
     */
	public VolatileShortArray(int length) {
        array = new short[length];
        // must perform at least one volatile write to conform to JMM
        if (length > 0)
            unsafe.putShortVolatile(array, rawIndex(0), (short)0);
	}
	
    /**
     * Creates a new VolatileShortArray with the same length as, and
     * all elements copied from, the given array.
     *
     * @param array the array to copy elements from
     * @throws NullPointerException if array is null
     */
    public VolatileShortArray(short[] array) {
        if (array == null)
            throw new NullPointerException();
        int length = array.length;
        if (length > 0) {
            int last = length-1;
            this.array = Arrays.copyOf(array, array.length);
            // Do the last write as volatile
            unsafe.putShortVolatile(this.array, rawIndex(last), array[last]);
        } else {
        	this.array = new short[0];
        }
    }
    
    /**
     * Creates a new VolatileShortArray of given length, and
     * set all elements with the given value.
     * 
      * @param length the length of the array
      * @param value the value with which the array elements are initialized
    */
    public VolatileShortArray(int length, short value) {
        array = new short[length];
        if (length > 0) {
            int last = length-1;
            Arrays.fill(array, 0, last, value);
            // Do the last write as volatile
            unsafe.putShortVolatile(this.array, rawIndex(last), array[last]);
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
    public final short get(int i) {
        return unsafe.getShortVolatile(array, rawIndex(i));
    }

    /**
     * Sets the element at position {@code i} to the given value.
     *
     * @param i the index
     * @param newValue the new value
     */
    public final void set(int i, short newValue) {
        unsafe.putShortVolatile(array, rawIndex(i), newValue);
    }

}
