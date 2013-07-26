package org.logicmill.util;

/** An object that produces 64-bit hash code values for instances of the
 * specified key type. Because there is no 64-bit cognate of 
 * {@link Object#hashCode()}, implementations of {@code LargeHashMap} 
 * require the programmer to provide a key adapter through which the map 
 * may obtain long hash codes from keys. Implementations of {@code 
 * LargeHashMap} must provide a means of associating a key adapter with a
 * map. Typically, a hash map constructor accepts a parameter of type 
 * {@code LargeHashMap.KeyAdapter}.<p>
 * Note that, although a  key adapter is used in the context of a hash map
 * with a specified key type ({@code LargeHashMap<K,V>}), the key parameter
 * is type {@code Object}. The contract for {@code getLongHashCode(Object)}
 * is inextricably intertwined with {@link java.lang.Object#equals(Object)}.
 * Like implementations of {@code equals()}; an implementation of 
 * {@code getLongHashCode(Object)} will, in most cases, share many features
 * with the implementation of {@code equals()} for the same class, including
 * type checking and casting.
 * 
 * @author David Curtis
 *
 */
public abstract class KeyAdapter {
	
	/** Compute a 64-bit hash code for the specified key.
	 * <p>The contract for {@code getLongHashCode(Object key)} is essentially the 
	 * same as {@link Object#hashCode()}:
	 * <ul> 
	 * <li> Whenever it is invoked on the same key more than once during 
	 * an execution of a Java application, {@code getHashCode()} 
	 * must consistently return the same {@code long} value, provided 
	 * no information used in {@code equals(Object)} comparisons on the 
	 * key is modified. This {@code long} value need not remain consistent
	 * from one execution of an application to another.
	 * <li> If two keys are equal according to {@code equals(Object)}, 
	 * then calling the {@code getHashCode(Object key)} method with each of 
	 * the two keys must produce the same result.
	 * <li> It is <i>not</i> required that if two keys are unequal 
	 * according to {@code Object.equals(Object)}, then calling the 
	 * {@code getHashCode()} method on each of the two keys must 
	 * produce distinct results. 
	 * @param key key from which a 64-bit hash code is computed
	 * @return the 64-bit hash code for the specified key
	 */
	public abstract long getLongHashCode(Object key);
	
	public boolean keyEquals(Object mappedKey, Object key) {
		return mappedKey.equals(key);
	}
}
