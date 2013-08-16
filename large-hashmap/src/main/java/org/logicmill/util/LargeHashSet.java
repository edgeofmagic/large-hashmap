package org.logicmill.util;

import java.util.Iterator;

public interface LargeHashSet<E> extends Iterable<E> {
	
	public interface EntryAdapter<E> {
		long getLongHashCode(Object entryKey);
		boolean entryMatches(E mappedEntry, Object entryKey);
	}

	boolean add(E entry);
	
	boolean contains(Object entryKey);

	boolean isEmpty();

	E remove(Object entryKey);

	long size();

	E get(Object entryKey);
	
	

}