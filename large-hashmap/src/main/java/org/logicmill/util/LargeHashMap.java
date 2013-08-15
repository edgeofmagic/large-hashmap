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
 * <p>This interface is based on {@link java.util.Map}{@code <K,V>}
 * and {@link java.util.concurrent.ConcurrentMap}{@code <K,V>}, but it
 * is specifically intended to support maps of very large size (greater than
 * 2<sup>32</sup> entries). It differs from {@code java.util.Map<K,V>} and 
 * {@code java.util.concurrent.ConcurrentMap<K,V>} in the following ways:
 * <ol>
 * <li>It omits methods that would entail a full traversal of the map in a 
 * single method, such as {@link java.util.Map#containsValue(Object)}.
 * <li>It omits methods that would return the contents of the entire 
 * map as anther collection view, such as {@link java.util.Map#entrySet()}.
 * <li>As an alternative to collection views, it provides an iterator
 * for map entries (associated key/value pairs).
 * <li>It employs 64-bit hash codes, by way of a <i>key adapter</i> object.
 * </ol>
 * The omissions are motivated by the impracticality of such operations on 
 * maps of extremely large size. Because of these differences, this interface 
 * and its implementations are not participants in the Java Collections 
 * Framework.<p>
 * <h4>Key adapters</h4>
 * Every instance of an implementation of {@code LargeHashMap} must have 
 * exactly one associated key adapter object that implements {@link 
 * LargeHashMap.KeyAdapter}{@code <K>}. The key adapter acts as an 
 * intermediary between the map and key objects. Specifically, it 
 * generates 64-bit hash codes from keys, and it compares keys to 
 * determine whether a key parameter (such as the parameter to 
 * {@code get(Object key)}) matches a key stored in the map. See
 * {@link LargeHashMap.KeyAdapter} for discussion of how to implement
 * key adapters.<p>
 * 
 * 
 * Implementations of {@code LargeHashMap} has certain responsibilities with
 * respect to key adapters:
 * <ul>
 * <li>Implementations must provide a mechanism for associating a key adapter
 * object with a hash map instance. Typically, a reference to a key adapter
 * would be passed as a parameter to the constructor of the map implementation.
 * <li> Whenever an implementation requires a hash code value from a key, the
 * map implementation must obtain it by invoking {@code 
 * KeyAdapter.getLongHashCode(Object key)} on the key adapter associated with
 * the map. Map implementations can rely on the immutability of {@link 
 * LargeHashMap.Entry} objects to guarantee that a key (and thus, its
 * associated hash code) will not change for the life time of the entry, 
 * permitting hash codes to be stored with keys to avoid the cost of
 * re-computing hash codes.
 * <li>Whenever an implementation needs to compare a keys for equivalence,
 * it must invoke {@code KeyAdapter.keyMatches(K mapKey, Object key)} on the
 * key adapter associated with the map, where {@code mapKey} is a key
 * stored in a map entry, and {@code key} is a key provided as a 
 * parameter to the method being executed. See {@link 
 * KeyAdapter#keyMatches(Object, Object)} for a detailed discussion.
 * The descriptions of methods on {@code LargeHashMap} that use keys
 * discuss the specific ways in which map implementations should rely
 * on key adapters for comparing keys.
 * </ul>
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
	public interface Entry<K,V> extends LongHashable {
		
		/** Returns the key corresponding to this entry.
		 * @return the key corresponding to this entry
		 */
		K getKey();
		
		/** Returns the value associated with the key corresponding to 
		 * this entry.
		 * @return the value corresponding to this entry
		 */
		V getValue();
	}
	
	/**
	 */
	
	/** An adapter object that acts as an intermediary between implementations 
	 * of {@code LargeHashMap} and classes used as keys in those 
	 * implementations. Key adapters perform two inter-related functions: they
	 * produce 64-bit hash codes from key objects, and they compare key values
	 * for equivalence.
	 * <h4>Long hash codes</h4>
	 * Lacking a 64-bit cognate of {@link Object#hashCode()}, {@code
	 * LargeHashMap} implementations require a generalized mechanism for 
	 * obtaining appropriate hash code value from keys. Every instance of a
	 * {@code LargeHashMap} implementation has exactly one associated key
	 * adapter object. Whenever a hash code is required for an operation on
	 * on the map, the map implementation invokes {@link 
	 * KeyAdapter#getLongHashCode(Object key)} with the key as its parameter.
	 * <h4>Key matching</h4>
	 * A hash map implementation will invoke 
	 * {@code KeyAdapter.keyMatches(K mapKey, Object key)} whenever a 
	 * comparison is made between a key already stored in the map, and a 
	 * key provided as a method parameter. Keys stored in hash map entries
	 * will always be instances of the parameterized key type {@code <K>}
	 * associated with the map. Keys passed as method parameters, when they
	 * are not subject to being put in the table (that is, key parameters
	 * in all methods except {@code put} and {@code putIfAbsent}) are
	 * passed as type {@code Object}. This fact can be exploited to relax type
	 * constraints for keys used in retrieval, removal, and value replacement
	 * methods. See the first example key adapter implementation below for
	 * an illustration of this capability.
	 * <h4>Key adapter examples</h4>
	 * The first example illustrates a key adapter for type {@code 
	 * String}:<pre><code>
	 *	public static class StringKeyAdapter extends LargeHashMap.KeyAdapter{@literal <}String{@literal >} {
	 *		{@literal @}Override
	 *		public long getLongHashCode(Object key) {
	 *			return org.logicmill.util.hash.SpookyHash64.hash((CharSequence)key,  0L);
	 *			// if the cast fails, ClassCastException will be thrown
	 *		}
	 *		{@literal @}Override
	 *		public boolean keyMatches(String mappedKey, Object key) {
	 *			if (key instanceof String) {
	 *				return mappedKey.equals(key);
	 *			} else if (key instanceof CharSequence) {
	 *				return mappedKey.contentEquals((CharSequence)key);
	 *			} else {
	 *				return false;
	 *			}
	 *		}
	 *	}
	 *</code></pre>
	 *
	 * Given this key adapter implementation, map entries can be retrieved, 
	 * removed, or tested for presence with keys of any type that support
	 * the {@link CharSequence} interface (such as {@link StringBuffer}, 
	 * {@link java.nio.CharBuffer}, or {@link StringBuilder}).<p>
	 * The second example illustrates an adapter for keys of type {@code 
	 * byte[]}:<pre><code>
	 *	public static class ByteKeyAdapter implements LargeHashMap.KeyAdapter{@literal <}byte[]{@literal >} {
	 *		{@literal @}Override
	 *		public long getLongHashCode(Object key) {
	 *			return org.logicmill.util.hash.SpookyHash64.hash((byte[])key,  0L);							
	 *		}
	 *	
	 *		{@literal @}Override
	 *		public boolean keyMatches(byte[] mappedKey, Object key) {
	 *			if (key instanceof byte[]) {
	 *				byte[] byteKey = (byte[])key;
	 *				if (mappedKey.length != byteKey.length) return false;
	 *				for (int i = 0; i < byteKey.length; i++) {
	 *					if (mappedKey[i] != byteKey[i]) return false;
	 *				}
	 *				return true;
	 *			} else return false;
	 *		}
	 *	}
	 *</code></pre>
	 * Because the key adapter assumes responsibility for key matching, it can
	 * implement value-based comparisons on arrays of primitive types.
	 * 
	 * @author David Curtis
	 *
	 * @param <K> The type of key associated with this adapter
	 */
	public interface KeyAdapter<K> {
		
		/** Returns a 64-bit hash code for the specified key. Implementations
		 * of this method must provide the following guarantees:
		 * <ul>
		 * <li>If {@code keyMatch(K mapKey, Object key)} returns {@code true} for
		 * keys {@code mapKey} and {@code key}, then {@code 
		 * getLongHashCode(mapMey)} and {@code getLongHashCode(key)} must
		 * return the same value.
		 * <li>If {@code getLongHashCode(Object key)} is invoked with the same 
		 * key parameter multiple times during the execution of a Java 
		 * application, it must consistently return the same {@code long} 
		 * value, provided no information used in {@code keyMatches} 
		 * comparisons with other keys is modified. This value need not remain 
		 * consistent from one execution of an application to another.
		 * </ul>
		 * @param key key for which the hash code is returned
		 * @return 64-bit hash code for the specified key
		 */
		long getLongHashCode(Object key);
		
		/** Returns true if the specified keys are considered to match for the
		 * purposes of retrieval from a hash map.
		 * <p>
		 * The definition of key <i>matching</i> is not identical to object 
		 * <i>equality</i>, as determined by {@code Object.equals(Object)}. 
		 * For example, symmetry is not a required property, since the
		 * types of the key parameters to 
		 * {@code keyMatches(K mapKey, Object key)} may not allow mutual 
		 * substitution (that is, {@code key} may not be capable of being
		 * cast to type {@code K}). 
		 * To a great extent, the precise definition of key matching is left
		 * to the programmer, based on the intended use of keys in the context 
		 * of a particular application using a hash map. The following 
		 * recommendations should be considered when implementing key adapters:
		 * <ul>
		 * <li>{@code keyMatch(k, k)} should return {@code true} for any 
		 * non-null key {@code k} (that is, is should be reflexive).
		 * <li>If possible, based on run-time types of the parameters, 
		 * {@code keyMatch} should devolve to {@code equals(Object)}, as 
		 * illustrated in the first example, above.
		 * <li>Comparisons should be based on compatible information models.
		 * Also in the first example above, note that a comparison of a 
		 * {@code String} with a {@code CharSequence} relies on 
		 * {@code String.contentEquals} to do an appropriate comparison of 
		 * compatible information, where {@code String.equals} would fail even 
		 * if the underlying character sequences were identical.
		 * </ul>
		 * 
		 * @param mapKey key value stored in the map
		 * @param key key parameter to be compared with {@code mapKey}
		 * @return true if the keys match
		 */
		boolean keyMatches(K mapKey, Object key);
	}
	
	/**
	 * Returns {@code true} if this map contains a mapping for the specified 
	 * key. More formally, returns {@code true} f this map contains a mapping 
	 * from a key {@code k} to a value {@code v} such that<pre><code> 
	 *	adapter.keyMatches(k, key) == true
	 * </code></pre>where {@code adapter} is the key adapter associated with this map.
	 * @param key key whose presence in this map is to be tested
	 * @return {@code true} if this map contains a mapping for the specified key 
	 * @throws NullPointerException if {@code key} is null
	 */
	boolean containsKey(Object key);
	
	/** Returns the value to which the specified key is mapped, or {@code null} 
	 * if this map contains no mapping for the key. More formally, if this map 
	 * contains a mapping from a key {@code k} to a value {@code v} such 
	 * that<pre><code> 
	 *	adapter.keyMatches(k, key) == true
	 * </code></pre>(where {@code adapter} is the key adapter associated with this map)
	 * then this method returns {@code v}; otherwise it returns 
	 * {@code null}. There can be at most one such mapping.
	 * @param key the key whose associated value is to be returned 
	 * @return the value to which the specified key is mapped, or {@code null}
	 * if this map contains no mapping for the key
	 * @throws NullPointerException if {@code key} is {@code null}
	 */
	V get(Object key);
	
	
	/**
	 * @param key
	 * @return and entry containing the matching key and its associated value
	 */
	Entry<K,V> getEntry(Object key);

	/** Associates the specified value with the specified key in this map, only
	 * if the specified key is not already present in the map. This is 
	 * equivalent to<pre><code>
	 *	if (!map.containsKey(key)) {
	 *		return map.put(key,value);
	 *	} else return map.get(key);</code></pre>
	 * except that the action is performed atomically.
	 * 
	 * @param key key with which the specified value is to be associated
	 * @param value value to be associated with the specified key
	 * @return the previous value associated with the specified key, or 
	 * {@code null} if there was no mapping for the key 
	 * @throws NullPointerException if {@code key} or {@code value} is {@code null}
	 */
	V putIfAbsent(K key, V value);
	
	
	/** Associates the specified value with the specified key in this map. If 
	 * the map previously contained a mapping for the key (that is, if {@code 
	 * containsKey(key)} returns {@code true}), the old value is 
	 * replaced by the specified value.
	 * @param key key with which the specified value is to be associated
	 * @param value value to be associated with the specified key 
	 * @return the previous value associated with {@code key}, or {code@ null} if 
	 * there was no mapping for {@code key}
	 * @throws NullPointerException if {@code key} or {@code value} is {@code null}
	 */
	V put(K key, V value);

	/** Removes the mapping for the specified key from this map if it is 
	 * present. More formally, if this map contains a mapping from a key 
	 * {@code k} to a value {@code v} such that<pre><code> 
	 *	adapter.keyMatches(k, key) == true
	 * </code></pre>(where {@code adapter} is the key adapter associated with 
	 * this map) then that mapping is removed. Returns the value associated 
	 * with the specified key in this map, or {@code null} if the key was not
	 * present in the map.
	 * @param key the key whose mapping is to be removed from the map 
	 * @return the previous value associated with key, or {@code null} if 
	 * there was no mapping for key
	 * @throws NullPointerException if {@code key} is null
	 */
	V remove(Object key);
	
	/**
	 * Removes the entry for a key only if currently mapped to a given value. 
	 * This is equivalent to<pre><code>
	 *	if (map.containsKey(key) && map.get(key).equals(value)) {
	 *		map.remove(key);
	 *		return true;
	 *	} else return false;</code></pre>
	 * except that the action is performed atomically. 
	 * 
	 * @param key key with which the specified value is associated
	 * @param value value expected to be associated with the specified key 
	 * @return {@code true} if the value was removed
	 * @throws NullPointerException if {@code key} or {@code value} is {@code null}
	 */
	boolean remove(Object key, Object value);
	
	/**
	 * Replaces the entry for a key only if currently mapped to some value. 
	 * This is equivalent to
	 * <pre><code>
	 *	if (map.containsKey(key)) {
	 *		return map.put(map.getEntry(key).getKey(), value);
	 *	} else return null;</code></pre>
	 * except that the action is performed atomically.
	 * @param key key with which the specified value is associated
	 * @param value value to be associated with the specified key 
	 * @return the previous value associated with the specified key, or {@code null} if there was no mapping for the key. 
	 * @throws NullPointerException if {@code key} or {@code value} is {@code null}
	 */
	V replace(Object key, V value);
	
	/**
	 * Replaces the entry for a key only if currently mapped to a given value. 
	 * This is equivalent to<pre><code>
	 *	if (map.containsKey(key) && map.get(key).equals(oldValue)) {
	 *		map.put(map.getEntry(key).getKey(), newValue);
	 *		return true;
	 *	} else return false;</code></pre>
	 * except that the action is performed atomically, and with a lot less
	 * thrashing about than this description suggests. 
	 * @param key key with which the specified value is associated
	 * @param oldValue value expected to be associated with the specified key
	 * @param newValue value to be associated with the specified key 
	 * @return {@code true} if the value was replaced, {@code false} otherwise
	 * @throws NullPointerException if {@code key}, {@code oldValue} or 
	 * {@code newValue} is {@code null}
	 */
	boolean replace(Object key, Object oldValue, V newValue);
	
	/** Returns the number of key-value entries in this map.
	 * @return the number of key-value entries in this map
	 */
	long size();
	
	/**
	 * Returns {@code true} if this map contains no key-value mappings.
	 * @return {@code true} if this map contains no key-value mappings
	 */
	boolean isEmpty();

	/** Returns an iterator over the entries contained in this map.
	 * <p>There are no guarantees concerning the order in which the entries 
	 * are returned.
	 * @return an iterator over the entries in this map
	 */
	 Iterator<Entry<K,V>> getEntryIterator();
	 
	 /**
	 * @return
	 */
	LargeHashSet<Entry<K,V>> entrySet();

}