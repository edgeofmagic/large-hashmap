package org.logicmill.util.concurrent;

import org.logicmill.util.LargeHashMap;

/**
 * @author edgeofmagic
 *
 * @param <K>
 */
public abstract class AbstractKeyAdapter<K> implements LargeHashMap.KeyAdapter<K> {

	@Override
	public abstract long getKeyHashCode(Object key);
	
	@Override
	public long getLongHashCode(Object key) {
		if (key instanceof LargeHashMap.Entry<?, ?>) {
			return ((LargeHashMap.Entry<?, ?>)key).getLongHashCode();
		} else {
			return getKeyHashCode(key);
		}
	}

	@Override
	public abstract boolean keyMatches(Object key, K mapKey);

	@Override
	public boolean matches(Object obj,
			LargeHashMap.Entry<K, ?> entry) {
		if (obj instanceof LargeHashMap.Entry<?, ?>) {
			return this.keyMatches(((LargeHashMap.Entry<?,?>)obj).getKey(), entry.getKey());
		} else {
			return this.keyMatches(obj, entry.getKey());
		}
	}

}
