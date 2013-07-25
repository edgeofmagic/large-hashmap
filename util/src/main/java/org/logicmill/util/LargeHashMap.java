/*
 * Copyright 2013 David Curtis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.logicmill.util;

import java.util.Iterator;

/**
 * An object that maps keys to values. A map cannot contain duplicate keys; 
 * each key can map to at most one value.
 * <p>This interface is an abbreviated version of 
 * {@link java.util.Map}{@code <K,V>}, appropriate for large hash maps.
 * It differs from {@code java.util.Map<K,V>} in the following ways:
 * <ol>
 * <li>It omits methods that would entail a full traversal of the map in a 
 * single method, such as {@link java.util.Map#containsValue(Object)}.
 * <li>It omits methods that would return the contents of the entire 
 * map as other collection views, such as {@link java.util.Map#entrySet()}.
 * <li>As an alternative to collection views, it provides iterators
 * for keys, values, and entries (associated key/value pairs).
 * <li>It supports 64-bit hash code values by requiring the programmer
 * to provide a <i>key adapter</i> implementing 
 * {@link LargeHashMap.KeyAdapter}{@code <K>},
 * to compute {@code long} hash codes for instances of the key type.
 * </ol>
 * The omissions are motivated by the impracticality of such operations on 
 * maps of extremely large size. Consequently, this interface and its 
 * implementations are not participants in the Java Collections Framework.<p>
 * Implementations of LargeHashMap must not allow {@code null} to be used 
 * as a key or value.
 * 
 * @author David Curtis
 *
 * @param <K> type of the keys maintained in this map
 * @param <V> type of the mapped values
 */
public interface LargeHashMap<K, V> {
	
	/** A map entry that encapsulates a key/value pair. An entry is created by
	 * the map implementation when a new key/value association is added to the
	 * map. Valid entries can only be obtained from an iterator returned by 
	 * {@code LargeHashMap.getEntryIterator()}.
	 * @author David Curtis
	 *
	 * @param <K> type of keys associated with values in this map entry
	 * @param <V> type of values associated with keys in this map entry
	 */
	public interface Entry<K,V> {
		
		/** Returns the key corresponding to this entry.
		 * @return the key corresponding to this entry
		 */
		public K getKey();
		
		/** Returns the value associated with the key corresponding to 
		 * this entry.
		 * @return the value corresponding to this entry
		 */
		public V getValue();
	}
	
	/** An object that produces 64-bit hash code values for instances of the
	 * specified key type. Because there is no 64-bit cognate of 
	 * {@link Object#hashCode()}, implementations of {@code LargeHashMap} 
	 * require the programmer to provide a key adapter through which the map 
	 * may obtain long hash codes from keys. Implementations of {@code 
	 * LargeHashMap} must provide a means of associating a key adapter with a
	 * map. Typically, a hash map constructor accepts a parameter of type 
	 * {@code LargeHashMap.KeyAdapter<K>}.
	 * 
	 * @author David Curtis
	 *
	 * @param <K> the type of key for which long hash codes are computed
	 */
	public interface KeyAdapter {
		
		/** Compute a 64-bit hash code for the specified key.
		 * <p>The contract for {@code getHashCode(K key)} is essentially the 
		 * same as {@link Object#hashCode()}:
		 * <ul> 
		 * <li> Whenever it is invoked on the same key more than once during 
		 * an execution of a Java application, {@code getHashCode()} 
		 * must consistently return the same {@code long} value, provided 
		 * no information used in {@code equals(Object)} comparisons on the 
		 * key is modified. This {@code long} value need not remain consistent
		 * from one execution of an application to another.
		 * <li> If two keys are equal according to {@code equals(Object)}, 
		 * then calling the {@code getHashCode(K key)} method with each of 
		 * the two keys must produce the same result.
		 * <li> It is <i>not</i> required that if two keys are unequal 
		 * according to {@code Object.equals(Object)}, then calling the 
		 * {@code getHashCode()} method on each of the two keys must 
		 * produce distinct results. 
		 * @param key key from which a 64-bit hash code is computed
		 * @return the 64-bit hash code for the specified key
		 */
		public long getLongHashCode(Object key);
	}
	
	/**
	 * Returns {@code true} if this map contains a mapping for the specified key. Null
	 * key values are not permitted.
	 * @param key key whose presence in this map is to be tested
	 * @return {@code true} if this map contains a mapping for the specified key 
	 * @throws NullPointerException if {@code key} is null
	 */
	public boolean containsKey(Object key);
	
	/** Returns the value to which the specified key is mapped, or 
	 * {@code null} if this map contains no mapping for the key. 
	 * <p>More formally, if this map contains a mapping from a key {@code k} 
	 * to a value {@code v} such that {@code key.equals(k)}, then this method 
	 * returns {@code v}; otherwise it returns {@code null}. There can be at 
	 * most one such mapping.
	 * @param key the key whose associated value is to be returned 
	 * @return the value to which the specified key is mapped, or {@code null}
	 * if this map contains no mapping for the key
	 * @throws NullPointerException if {@code key} is {@code null}
	 */
	public V get(Object key);

	/** Associates the specified value with the specified key in this map, 
	 * if the specified key is not already associated with a value. 
	 * Neither the key nor the value can be null.
	 * @param key key with which the specified value is to be associated
	 * @param value value to be associated with the specified key
	 * @return the previous value associated with the specified key, or 
	 * {@code null} if there was no mapping for the key 
	 * @throws NullPointerException if {@code key} or {@code value} is {@code null}
	 */
	public V putIfAbsent(K key, V value);
	
	
	/** Associates the specified value with the specified key in this map. 
	 * Neither the key nor the value can be null. 
	 * @param key key with which the specified value is to be associated
	 * @param value value to be associated with the specified key 
	 * @return the previous value associated with {@code key}, or {code@ null} if 
	 * there was no mapping for {@code key}
	 * @throws NullPointerException if {@code key} or {@code value} is {@code null}
	 */
	public V put(K key, V value);

	/** Removes the mapping for the specified key from this map if it is 
	 * present. 
	 * <p>More formally, if this map contains a mapping from a key {@code k} 
	 * to a value {@code v} such that {@code key.equals(k))}, then that mapping
	 * is removed. Returns the value associated with the specified
	 * key in this map, or {@code null} if the map contained no association 
	 * for the key. 
	 * @param key the key whose mapping is to be removed from the map 
	 * @return the previous value associated with key, or {@code null} if 
	 * there was no mapping for key
	 * @throws NullPointerException if {@code key} is null
	 */
	public V remove(Object key);
	
	/**
	 * Removes the entry for a key only if currently mapped to a given value. 
	 * This is equivalent to 
	 * <pre><code>if (map.containsKey(key) && map.get(key).equals(value)) {
	 * 	map.remove(key);
	 * 	return true;
	 * } else return false;</code></pre>
	 * except that the action is performed atomically. 
	 * 
	 * @param key key with which the specified value is associated
	 * @param value value expected to be associated with the specified key 
	 * @return {@code true} if the value was removed
	 * @throws NullPointerException if {@code key} or {@code value} is {@code null}
	 */
	public boolean remove(Object key, Object value);
	
	/**
	 * Replaces the entry for a key only if currently mapped to some value. This is equivalent to
	 * <pre><code>if (map.containsKey(key)) {
	 * 	return map.put(key, value);
	 * } else return null;</code></pre>
	 * except that the action is performed atomically.
	 * @param key key with which the specified value is associated
	 * @param value value to be associated with the specified key 
	 * @return the previous value associated with the specified key, or {@code null} if there was no mapping for the key. 
	 * @throws NullPointerException if {@code key} or {@code value} is {@code null}
	 */
	public V replace(K key, V value);
	
	/**
	 * Replaces the entry for a key only if currently mapped to a given value. 
	 * This is equivalent to
	 * <pre><code>if (map.containsKey(key)} {
	 *  if (map.get(key).equals(oldValue) {
	 *  	map.put(key, newValue);
	 *  	return true;
	 * } else return false;</code></pre>
	 * except that the action is performed atomically. 
	 * @param key key with which the specified value is associated
	 * @param oldValue value expected to be associated with the specified key
	 * @param newValue value to be associated with the specified key 
	 * @return true if the value was replaced 
	 * @throws NullPointerException if {@code key}, {@code oldValue} or 
	 * {@code newValue} is {@code null}
	 */
	public boolean replace(K key, V oldValue, V newValue);
	
	/** Returns the number of key-value entries in this map.
	 * @return the number of key-value entries in this map
	 */
	public long size();
	
	/**
	 * Returns {@code true} if this map contains no key-value mappings.
	 * @return {@code true} if this map contains no key-value mappings
	 */
	public boolean isEmpty();

	/** Returns an iterator over the values contained in this map. 
	 * <p>There are no guarantees concerning the order in which the values 
	 * are returned.
	 * @return an iterator over the values in this map
	 */
	public Iterator<V> getValueIterator();

	/** Returns an iterator over the keys contained in this map. 
	 * <p>There are no guarantees concerning the order in which the keys 
	 * are returned.
	 * @return an iterator over the keys in this map
	 */
	public Iterator<K> getKeyIterator();
	
	/** Returns an iterator over the entries contained in this map.
	 * <p>There are no guarantees concerning the order in which the entries 
	 * are returned.
	 * @return an iterator over the entries in this map
	 */
	public Iterator<LargeHashMap.Entry<K,V>> getEntryIterator();

}