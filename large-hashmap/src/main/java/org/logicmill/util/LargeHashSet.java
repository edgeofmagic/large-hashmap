package org.logicmill.util;

import java.util.Iterator;

public interface LargeHashSet<E> extends Iterable<E> {
	
/*	public interface EntryAdapter<E> {
			long getLongHashCode(Object entryKey);
			boolean entryMatches(E mappedEntry, Object entryKey);
		}
*/
	boolean add(E entry);
	
	boolean contains(Object entryKey);

	boolean isEmpty();
	
	
	/**
	 * @param entryKey
	 * @return
	 */
	E remove(Object entryKey);
	
	
//	E replace(Object entryKey, Object oldEntry, E newEntry);


	long size();

	E get(Object entryKey);
	
	/*
	 * If oldEntry is null, replace a matching entry (if any) with newEntry. If
	 * oldEntry is non-null, replace a matching entry only if that entry is
	 * the same object as oldEntry (that is, oldEntry == mappedEntry) or if
	 * that entry equals oldEntry (mappedEntry.equals(oldEntry)). Returns
	 * the replaces entry if the replacement was successful.
	 */
	

}