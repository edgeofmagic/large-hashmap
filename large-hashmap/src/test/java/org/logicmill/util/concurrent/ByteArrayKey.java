package org.logicmill.util.concurrent;

import java.util.Arrays;

import org.logicmill.util.hash.SpookyHash64;

public class ByteArrayKey {
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
	
	public int size() {
		return array.length;
	}
}
