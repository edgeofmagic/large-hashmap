package org.logicmill.util;

public interface TypeAdapter<T> {

	long getLongHashCode(Object object);

	boolean matches(Object obj, T entry);

	
}
