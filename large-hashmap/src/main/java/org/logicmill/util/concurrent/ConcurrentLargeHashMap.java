package org.logicmill.util.concurrent;

import java.util.Iterator;

import org.logicmill.util.LargeHashMap;
import org.logicmill.util.LargeHashCore;
import org.logicmill.util.LongHashable;

public class ConcurrentLargeHashMap<K,V> implements LargeHashMap<K, V> {
	
	private final LargeHashCore<LargeHashMap.Entry<K,V>> hashCore;
	private final LargeHashMap.KeyAdapter<K> keyAdapter;
	
	public ConcurrentLargeHashMap(int segSize, int initSegCount, float loadFactorThreshold, LargeHashMap.KeyAdapter<K> keyAdapter) {
		hashCore = new ConcurrentLargeHashCore<LargeHashMap.Entry<K,V>>(segSize, initSegCount, loadFactorThreshold, new SetKeyAdapter(keyAdapter));
		this.keyAdapter = keyAdapter;
	}
	
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
	
	private class SetKeyAdapter implements LargeHashCore.EntryAdapter<LargeHashMap.Entry<K,V>> {
		
		private final LargeHashMap.KeyAdapter<K> mapKeyAdapter;
		
		private SetKeyAdapter(LargeHashMap.KeyAdapter<K> mapKeyAdapter) {
			this.mapKeyAdapter = mapKeyAdapter;
		}

		@Override
		public long getLongHashCode(Object key) {		
			return mapKeyAdapter.getLongHashCode(key);
		}

		@Override
		public boolean keyMatches(LargeHashMap.Entry<K,V> mappedEntry, Object key) {
			return mapKeyAdapter.keyMatches(mappedEntry.getKey(), key);
		}

	}



	@Override
	public boolean containsKey(Object key) {
		return hashCore.contains(key);
	}

	@Override
	public V get(Object key) {
		LargeHashMap.Entry<K,V> result = hashCore.get(key);
		return result != null ? result.getValue() : null;
	}

	@Override
	public V putIfAbsent(K key, V value) {
		LargeHashMap.Entry<K,V> entry = new Entry<K,V>(key, value, keyAdapter.getLongHashCode(key));
		LargeHashMap.Entry<K,V> result =  hashCore.putIfAbsent(entry);
		return result != null ? result.getValue() : null ;
	}

	@Override
	public V put(K key, V value) {
		Entry<K,V> entry = new Entry<K,V>(key, value, keyAdapter.getLongHashCode(key));
		LargeHashMap.Entry<K,V> result =  hashCore.put(entry);
		return result != null ? result.getValue() : null ;
	}

	@Override
	public V remove(Object key) {
		LargeHashMap.Entry<K,V> result =  hashCore.remove(key);
		return result != null ? result.getValue() : null ;
	}
	
	private abstract class MapRemoveHandler implements LargeHashCore.RemoveHandler<LargeHashMap.Entry<K,V>> {
		
		protected final Object value;
		MapRemoveHandler(Object value) {
			this.value = value;
		}
		
		@Override
		public abstract boolean remove(LargeHashMap.Entry<K,V> mappedEntry);
	}

	@Override
	public boolean remove(Object key, Object value) {
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

	private abstract class MapReplaceHandler implements LargeHashCore.ReplaceHandler<LargeHashMap.Entry<K,V>> {
		protected final V newValue;
		MapReplaceHandler(V newValue) {
			this.newValue = newValue;
		}
		@Override
		public abstract Entry<K,V> replaceWith(LargeHashMap.Entry<K,V> mappedEntry);
	}
	
	@Override
	public V replace(Object key, V value) {
		LargeHashMap.Entry<K,V> result = hashCore.replace(key, 
		new MapReplaceHandler(value) {
			@Override
			public Entry<K,V> replaceWith(LargeHashMap.Entry<K,V> mappedEntry) {
				return new Entry<K,V>(mappedEntry.getKey(), newValue, ((Entry<K,V>)mappedEntry).getLongHashCode());
			}
			
		});
		return result.getValue();
	}

	private abstract class MapReplaceOldHandler implements LargeHashCore.ReplaceHandler<LargeHashMap.Entry<K,V>> {
		protected final V newValue;
		protected final Object oldValue;
		MapReplaceOldHandler(Object oldValue, V newValue) {
			this.oldValue = oldValue;
			this.newValue = newValue;
		}
		@Override
		public abstract LargeHashMap.Entry<K,V> replaceWith(LargeHashMap.Entry<K,V> mappedEntry);
	}
	
	
	@Override
	public boolean replace(Object key, Object oldValue, V newValue) {
		LargeHashMap.Entry<K,V> result = hashCore.replace(key, 
				new MapReplaceOldHandler(oldValue, newValue) {
					@Override
					public LargeHashMap.Entry<K,V> replaceWith(LargeHashMap.Entry<K,V> mappedEntry) {
						if (mappedEntry.getValue().equals(oldValue)) {
							return new Entry<K,V>(mappedEntry.getKey(), newValue, ((Entry<K,V>)mappedEntry).getLongHashCode());
						} else return null;
					}
				});
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

}
