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

/** An object that can return a 64-bit hash code. This interface is
 * intended to be used in conjunction with implementations of {@link
 * LargeHashMap}{@code <K,V>} and associated implementations of {@link 
 * LargeHashMap.KeyAdapter}{@code <K>}. Specifically, implementations of {@code 
 * LargeHashMap} may provide a default key adapter along these lines:<pre><code>
 *	public class DefaultKeyAdapter{@literal <}K{@literal >} 
 *	implements LargeHashMap.KeyAdapter{@literal <}K{@literal >} {
 *		{@literal @}Override
 *		public long getLongHashCode(Object key) {
 *			return ((LongHashable)key).getLongHashCode();
 *		}
 *		{@literal @}Override
 *		public boolean keyMatches(K mapKey, Object key) {
 *			return mapKey.equals(key);
 *		}
 *	}</code></pre>
 * When the responsibility for providing a 64-bit hash code is shifted
 * from the key adapter to the key class itself, then the responsibility
 * for determining equivalence likewise shifts from {@code 
 * KeyAdapter.keyMatches} to the {@code equals(Object)} method on the key
 * class. Thus, the contract for {@code getLongHashCode()} is essentially 
 * the same as {@link java.lang.Object#hashCode()}:
 * <ul> 
 * <li> Whenever it is invoked on the same object more than once during 
 * an execution of a Java application, the {@code getLongHashCode()} 
 * method must consistently return the same {@code long} value, provided 
 * no information used in {@code equals(Object)} comparisons on the 
 * object is modified. This long value need not remain consistent from 
 * one execution of an application to another.
 * <li> If two objects are equal according to the {@code equals(Object)} 
 * method, then calling the {@code getLongHashCode()} method on each of 
 * the two objects must produce the same long value result.
 * <li> It is <i>not</i> required that if two objects are unequal 
 * according to the {@code equals(Object)} method, then calling the 
 * {@code getLongHashCode()} method on each of the two objects must 
 * produce distinct results. 
 * </ul>
 * @author David Curtis
 * @see LargeHashMap.KeyAdapter
 * @see LargeHashMap
 *
 */
public interface LongHashable {
	
	/** Returns a 64-bit hash code value for the object. 
	 * @return a 64-bit hash code value for the object.
	 */
	public long getLongHashCode();

}
