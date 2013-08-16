package org.logicmill.util.concurrent;

import java.util.Iterator;

import org.logicmill.util.LargeHashMap;
import org.logicmill.util.LargeHashSet;
import org.logicmill.util.LongHashable;

/**
 * @author David Curtis
 *
 * @param <K>
 * @param <V>
 */
public class ConcurrentLargeHashMap<K,V> implements LargeHashMap<K, V> {
	
	private final ConcurrentLargeHashSet<LargeHashMap.Entry<K,V>> hashCore;
	private final LargeHashMap.KeyAdapter<K> keyAdapter;
	
	/**
	 * @param segSize
	 * @param initSegCount
	 * @param loadFactorThreshold
	 * @param keyAdapter
	 */
	public ConcurrentLargeHashMap(int segSize, int initSegCount, float loadFactorThreshold, LargeHashMap.KeyAdapter<K> keyAdapter) {
		hashCore = new ConcurrentLargeHashSet<LargeHashMap.Entry<K,V>>(segSize, initSegCount, loadFactorThreshold, new SetKeyAdapter(keyAdapter));
		this.keyAdapter = keyAdapter;
	}
	
	/**
	 * @param segSize
	 * @param initSegCount
	 * @param loadFactorThreshold
	 */
	public ConcurrentLargeHashMap(int segSize, int initSegCount, float loadFactorThreshold) {
		this(segSize, initSegCount, loadFactorThreshold, new DefaultKeyAdapter<K>());
	}
	
	/*
	 * Default entry implementation, stores hash codes to avoid repeatedly 
	 * computing them.
	 */
	private static class Entry<K,V> implements LargeHashMap.Entry<K, V>, LongHashable {
		private final K key;
		private final V value;
		private final long hashCode;
		
		private Entry(K key, V value, long hashCode) {
			this.key = key;
			this.value = value;
			this.hashCode = hashCode;
		}
		
		@Override
		public K getKey() {
			return key;
		}
		
		@Override
		public V getValue() {
			return value;
		}
		
		@Override
		public long getLongHashCode() {
			return hashCode;
		}
		
		
		
	}
	
	/*
	 * Default key adapter. 
	 */
	private static class DefaultKeyAdapter<K> implements LargeHashMap.KeyAdapter<K> {

		@Override
		public long getLongHashCode(Object key) {
			if (key instanceof LongHashable) {
				return ((LongHashable)key).getLongHashCode();
			} else {
				throw new IllegalArgumentException("key must implement org.logicmill.util.LongHashable");
			}
		}

		@Override
		public boolean keyMatches(K mapKey, Object key) {
			return mapKey.equals(key);
		}
	}
	
	private class SetKeyAdapter implements LargeHashSet.EntryAdapter<LargeHashMap.Entry<K,V>> {
		
		private final LargeHashMap.KeyAdapter<K> mapKeyAdapter;
		
		private SetKeyAdapter(LargeHashMap.KeyAdapter<K> mapKeyAdapter) {
			this.mapKeyAdapter = mapKeyAdapter;
		}

		@Override
		public long getLongHashCode(Object key) {	
			if (key instanceof Entry<?,?>) {
				return ((LongHashable) key).getLongHashCode();
			} else {
				return mapKeyAdapter.getLongHashCode(key);
			}
		}

		@Override
		public boolean entryMatches(LargeHashMap.Entry<K,V> mappedEntry, Object key) {
			if (key instanceof Entry<?,?>) {
				return mapKeyAdapter.keyMatches(mappedEntry.getKey(), ((Entry<?,?>)key).getKey());
			} else {
				return mapKeyAdapter.keyMatches(mappedEntry.getKey(), key);
			}
		}

	}



	@Override
	public boolean containsKey(Object key) {
		if (key == null) throw new NullPointerException();
		return hashCore.contains(key);
	}

	@Override
	public V get(Object key) {
		if (key == null) throw new NullPointerException();
		LargeHashMap.Entry<K,V> result = hashCore.get(key);
		return result != null ? result.getValue() : null;
	}

	@Override
	public V putIfAbsent(K key, V value) {
		if (key == null || value == null) throw new NullPointerException();
		LargeHashMap.Entry<K,V> entry = new Entry<K,V>(key, value, keyAdapter.getLongHashCode(key));
		LargeHashMap.Entry<K,V> result =  hashCore.putIfAbsent(entry);
		return result != null ? result.getValue() : null ;
	}

	@Override
	public V put(K key, V value) {
		if (key == null || value == null) throw new NullPointerException();
		Entry<K,V> entry = new Entry<K,V>(key, value, keyAdapter.getLongHashCode(key));
		LargeHashMap.Entry<K,V> result =  hashCore.put(entry);
		return result != null ? result.getValue() : null ;
	}

	@Override
	public V remove(Object key) {
		if (key == null) throw new NullPointerException();
		LargeHashMap.Entry<K,V> result =  hashCore.remove(key);
		return result != null ? result.getValue() : null ;
	}
	
	private abstract class MapRemoveHandler extends ConcurrentLargeHashSet.RemoveHandler<LargeHashMap.Entry<K,V>> {
		
		protected final Object value;
		MapRemoveHandler(Object value) {
			this.value = value;
		}
		
		@Override
		public abstract boolean remove(LargeHashMap.Entry<K,V> mappedEntry);
	}

	@Override
	public boolean remove(Object key, Object value) {
		if (key == null || value == null) throw new NullPointerException();
		LargeHashMap.Entry<K,V> result = hashCore.remove(key, 
		new MapRemoveHandler(value) {
			@Override
			public boolean remove(LargeHashMap.Entry<K, V> mappedEntry) {
				if (mappedEntry.getValue().equals(this.value)) {
					return true;
				} else {
					return false;
				}
			}
		});
		return result != null;
	}

	private class MapReplaceHandler extends ConcurrentLargeHashSet.ReplaceHandler<LargeHashMap.Entry<K,V>> {
		protected final V newValue;
		MapReplaceHandler(V newValue) {
			this.newValue = newValue;
		}
		@Override
		public Entry<K,V> replaceWith(LargeHashMap.Entry<K,V> mappedEntry) {
			return new Entry<K,V>(mappedEntry.getKey(), newValue, ((Entry<K,V>)mappedEntry).getLongHashCode());

		}
	}
	
	@Override
	public V replace(Object key, V value) {
		if (key == null || value == null) throw new NullPointerException();
		LargeHashMap.Entry<K,V> result = hashCore.replace(key, new MapReplaceHandler(value));
		return result != null ? result.getValue() : null;
	}

	private class MapReplaceOldHandler extends ConcurrentLargeHashSet.ReplaceHandler<LargeHashMap.Entry<K,V>> {
		protected final V newValue;
		protected final Object oldValue;
		MapReplaceOldHandler(Object oldValue, V newValue) {
			this.oldValue = oldValue;
			this.newValue = newValue;
		}
		@Override
		public LargeHashMap.Entry<K,V> replaceWith(LargeHashMap.Entry<K,V> mappedEntry) {
			if (mappedEntry.getValue().equals(oldValue)) {
				return new Entry<K,V>(mappedEntry.getKey(), newValue, ((Entry<K,V>)mappedEntry).getLongHashCode());
			} else return null;
		}
	}
	
	
	@Override
	public boolean replace(Object key, Object oldValue, V newValue) {
		if (key == null || oldValue == null || newValue == null) throw new NullPointerException();
		LargeHashMap.Entry<K,V> result = hashCore.replace(key, new MapReplaceOldHandler(oldValue, newValue));
		return result != null;
	}

	@Override
	public long size() {
		return hashCore.size();
	}

	@Override
	public boolean isEmpty() {
		return hashCore.isEmpty();
	}

	@Override
	public Iterator<LargeHashMap.Entry<K, V>> getEntryIterator() {
		return hashCore.iterator();
	}

	@Override
	public LargeHashMap.Entry<K, V> getEntry(Object key) {
		return hashCore.get(key);
	}
	
	@Override
	 public LargeHashSet<LargeHashMap.Entry<K,V>> entrySet()  {
		return hashCore;
	}


}
