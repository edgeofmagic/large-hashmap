package org.logicmill.util;

import java.util.Iterator;

public interface LargeHashCore<E> {
	
	public interface EntryAdapter<E> {
		
		long getLongHashCode(Object entryKey);
		
		boolean keyMatches(E mappedEntry, Object entryKey);
	}
	
	public interface RemoveHandler<E> {
		boolean remove(E mappedEntry);
	}
	
	public interface ReplaceHandler<E> {
		E replaceWith(E mappedEntry);
	}
	
	E put(E entry);
	
	E putIfAbsent(E entry);
	
	boolean contains(Object entryKey);
	
	boolean isEmpty();
	
	Iterator<E> iterator();
	
	E remove(Object entryKey);
	
	E remove(Object entryKey, RemoveHandler<E> handler);
			
	E replace(Object entryKey, ReplaceHandler<E> handler);
	
	E replace(Object entryKey, E newEntry);
	
	long size();

	E get(Object entryKey);

	
	
}
