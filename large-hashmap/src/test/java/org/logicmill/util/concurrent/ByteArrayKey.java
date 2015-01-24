package org.logicmill.util.concurrent;

import java.io.Serializable;
import java.util.Arrays;

import org.logicmill.util.hash.SpookyHash64;

/**
 * @author edgeofmagic
 *
 */
public class ByteArrayKey implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3481444374425224498L;
	private final byte[] array;
	public ByteArrayKey(byte[] bytes) {
		array = Arrays.copyOf(bytes, bytes.length);
	}
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ByteArrayKey) {
			ByteArrayKey key = (ByteArrayKey)obj;
			if (Arrays.equals(array, key.array)) {
				return true;
			}
		}
		return false;
	}
	@Override
	public int hashCode() {
		return (int) SpookyHash64.hash(array, 1337L);
	}
	
	public byte getByte(int index) {
		return array[index];
	}
	
	public int size() {
		return array.length;
	}
}
