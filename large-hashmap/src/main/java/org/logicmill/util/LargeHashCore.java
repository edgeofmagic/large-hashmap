package org.logicmill.util;

/** A utility providing generalized hash table mechanics, useful as a basis for constructing implementations
 * of specific functional hashed collections, such as hash map and hash set.
 * 
 * @author David Curtis
 *
 * @param <E> type of entries stored in the table
 */
public interface LargeHashCore<E> extends Iterable<E> {

	/** A helper class that determines whether values match, for the purposes 
	 * of locating elements within a table.
	 * 
	 * @author David Curtis
	 *
	 * @param <E> type of entries in the associated table
	 */
//	interface EntryAdapter<E> {
		/** Returns a hash code value for the {@code entryKey} parameter object.
		 * The contract for {@code getLongHashCode} is similar to that for {@link Object.hashCode}:
		 * <ul>
		 * <li>Whenever it is invoked on the same object more than once during an execution of a 
		 * Java application, the hashCode method must consistently return the same long value, 
		 * provided no information used in matching comparisons on the object is modified. 
		 * This value need not remain consistent from one execution of an application to 
		 * another execution of the same application.
		 * </li>
		 * <li>If two objects match according to the {@code entryMatches(E entry, Object key)} method, then 
		 * calling the hashCode method on each of the two objects must produce the same 
		 * long result.
		 * </li>
		 * <li>It is not required that if two objects do not match according to the 
		 * {@code entryMatches(E entry, Object key)} method, then calling the {code getLongHashCode} 
		 * method on each of the two objects must produce distinct long results. However, 
		 * the programmer should be aware that producing distinct values for non-matching 
		 * objects may improve the performance of hash tables.
		 * <li>
		 * </ul>
		 * @param entryKey object whose hash code is returned
		 * @return the hash code for {@code entryKey}
		 */
//		long getLongHashCode(Object entryKey);
		
		
		/** Indicates whether an object 'matches' an entry. The contact for 'match' is less
		 * strict than the contract for {@code Object.equals()}. 
		 * <ul>
		 * <li>
		 * Equality (defined by {@code Object.equals()}) implies matching: for non-null references {@code x} and
		 * {@code y}, if {@code x.equals(y)} is {@code true}, then {@code handler.entryMatches(x,y)} is 
		 * {@code true}. 
		 * </li>
		 * <li> As a relationship from {@code entryKey} to {@code matchingEntry}, matching is <i>functional</i> within the 
		 * scope of a hash table: for any value
		 * {@code entryKey}, at most one value {@code matchingEntry} in a hashTable can match.
		 * </li> Matching implies hash code equality: for non-null references {@code x} and
		 * {@code y}, if {@code handler.entryMatches(x,y)} is {@code true}, then 
		 * {@code handler.getLongHashCode(x) == handler.getLongHashCode(y)}.
		 * <li> For any non-null reference {@code y}, then {@code handler.entryMatches(null,y)} is 
		 * {@code false}.
		 * </li>
		 * </ul>
		 * @param entryKey value compared to matchingEntry for matching purposes 
		 * @param matchingEntry an entry in the associated hash table
		 * @return {@code true} if {@code entryKey} matches {@code matchingEntry}, {@code false} otherwise
		 */
//		boolean entryMatches(Object entryKey, E matchingEntry);
//	}

	/** A helper object that determines whether an entry should be removed. Employed
	 * by an implementation of {@link HashCore.replace(Object, ReplaceHandler<E>)}. 
	 * @author David Curtis
	 *
	 * @param <E> type of entries in the associated table
	 */
	interface RemoveHandler<E> extends MatchHandler<E> {
		/** Determine whether an existing entry should be removed.
		 * @param matchingEntry the entry subject to conditional removal
		 * @return true if the entry should be removed, false otherwise.
		 */
		boolean remove(E matchingEntry);
	}
	
	/** A helper object that produces an entry to replace an existing entry in the table.
	 * Invoked by{@link  HashCore.replace(Object, ReplaceHandler<E>)}. 
	 * @author David Curtis
	 *
	 * @param <E> type of entries in the associated table
	 */
	interface ReplaceHandler<E> extends MatchHandler<E> {
		
		/** Provide an entry to replace an existing entry, or null if no
		 * the existing entry should not be replaced.
		 * @param matchingEntry entry to be replaced
		 * @return new entry to replace {@code matchingEntry}, or null if no replacement
		 */
		E replaceWith(E matchingEntry);
	}
	
	/** A helper object that produces a new entry for insertion into the table. Used by 
	 * {@link LargeHashCore<E>.putIfAbsent()}. 
	 * @author David Curtis
	 *
	 * @param <E> type of entries in the associated table
	 */
//	interface PutHandler<E> {
		
		/** Provide an entry for insertion into the table.  
		 * Invoked by {@link LargeHashCore<E>.putIfAbsent()}, only if
		 * no matching entry was found.
		 * 
		 * @param entryKey value that matches (as defined by EntryAdapter) a putative instance of entry type E
		 * @return the entry to be inserted
		 */
//		E put(Object entryKey);
//	}
	
	interface PutHandler<E> extends MatchHandler<E> {
		E getNewEntry();
	}
	
	interface MatchHandler<E> {
		boolean match(E mappedEntry);
		long getLongHashCode();		
	}
	
	/** Conditionally replaces an entry. If an element matching entryKey is found in
	 * the table, invoke handler.replace(). If that invocation returns an entry value,
	 * that entry replaces the existing entry.
	 * 
	 * @param entryKey value that matches (as defined by EntryAdapter) a putative instance of entry type E
	 * @param handler an object that determines whether an existing entry is replaced, and provides the replacement
	 * @return the replaced entry previously in the table, or null if there was no matching entry or the handler returned null
	 */
	E replace(ReplaceHandler<E> handler);
	
	/**
	 * @param entryKey
	 * @param entry
	 * @return
	 */
//	E replace(Object entryKey, E entry);

	
	/** Conditionally removes an entry. If an element matching
	 * {@code entryKey} is found in the table, and {@code handler} is non-null, 
	 * remove the matching entry if {@code handler.remove()} returns {@code true}. If {@code handler}
	 * is null, the matching entry is removed unconditionally.
	 * 
	 * @param entryKey value that matches (as defined by EntryAdapter) a putative instance of entry type E
	 * @param handler an object that determines whether a matching entry is removed
	 * @return the removed entry, or null of no entry was removed
	 */
	E remove(RemoveHandler<E> handler);
	
	
	/** Conditionally inserts an entry into the table. 
	 * If no element that matches entry is present in the table, 
	 * insert entry. If a matching element is present, then replace
	 * it with entry if replaceIfPresent is true.
	 * 
	 * @param entry the entry to be inserted
	 * @param replaceIfPresent if true, any matching element in the table will be replaced
	 * @return null if the table did not contain a matching element, otherwise, return the matching element
	 */
	E put(PutHandler<E> handler, boolean replaceIfPresent);
	
	/** Conditionally provides and inserts an entry into the table.
	 * If no entry matching entryKey is present in the table, invoke handler.put() to 
	 * create a new entry and insert it. The handler is invoked within the scope of the
	 * atomic putIfAbsent operation. 
	 * 
	 * Note that there may not be a reliable way to determine whether the returned
	 * entry was previously in the table or a new entry.
	 * 
	 * @param entryKey value that matches (as defined by EntryAdapter) a putative instance of entry type E
	 * @param handler a factory for entries, invoked when an entry is required for insertion
	 * @return either the entry already in the table, or the new entry inserted
	 */
//	E putIfAbsent(Object entryKey, PutHandler<E> handler);
	
//	E putIfAbsent(PutHandler<E> handler);

	/** Returns the entry matching {@code entryKey}, or {@code null} 
	 * if no matching entry is found. There can be at most one such entry.
	 * @param entryKey value matching a putative entry in the hash table
	 * @return the matching entry, or null
	 */
//	E get(Object entryKey);
	
	E get(MatchHandler<E> handler);
	
	/** Returns the number of entries in the table.
	 * @return number of entries in the table
	 */
	long size();
	
	/** Returns an iterator over the entries contained in this table.
	 * <p>There are no guarantees concerning the order in which the entries 
	 * are returned.
	 * @return an iterator over the entries in this table
	 */	
}
