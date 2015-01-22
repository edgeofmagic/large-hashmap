package org.logicmill.util.concurrent;

import java.util.Iterator;

import org.logicmill.util.LargeHashCore;
import org.logicmill.util.LargeHashCore.PutHandler;
import org.logicmill.util.LargeHashMap;
import org.logicmill.util.LargeHashSet;
import org.logicmill.util.LongHashable;
import org.logicmill.util.TypeAdapter;

/**
 * @author David Curtis
 *
 * @param <K>
 * @param <V>
 */
public class ConcurrentLargeHashMap<K,V> implements LargeHashMap<K, V> {
	
	private final LargeHashCore<LargeHashMap.Entry<K,V>> hashCore;
	private final LargeHashMap.KeyAdapter<K> keyAdapter; // only used locally for hashCode, should work for key or map adapter
	
	/**
	 * @param segSize
	 * @param initSegCount
	 * @param loadFactorThreshold
	 * @param keyAdapter
	 */
	public ConcurrentLargeHashMap(int segSize, int initSegCount,
			float loadFactorThreshold, LargeHashMap.KeyAdapter<K> keyAdapter) {
		hashCore = new ConcurrentLargeHashCore<LargeHashMap.Entry<K,V>>(
				segSize, initSegCount, loadFactorThreshold, (TypeAdapter)keyAdapter);
		this.keyAdapter = keyAdapter;
	}
	
	/**
	 * @param segSize
	 * @param initSegCount
	 * @param loadFactorThreshold
	 */
	public ConcurrentLargeHashMap(int segSize, int initSegCount, float loadFactorThreshold) {
//		this(segSize, initSegCount, loadFactorThreshold, new DefaultMapAdapter());
		
		LargeHashMap.KeyAdapter<K> adapter = new DefaultKeyAdapter<K>();
//		TypeAdapter<LargeHashMap.Entry<K,V>> adapter = (TypeAdapter) keyAdap;
		hashCore = new ConcurrentLargeHashCore<LargeHashMap.Entry<K, V>> ( 
				segSize, initSegCount, loadFactorThreshold, (TypeAdapter)adapter);
		this.keyAdapter = adapter;		
		
	}
	
	/*
	 * Default entry implementation, stores hash codes to avoid repeatedly 
	 * computing them.
	 */
	private static class Entry<K,V> implements LargeHashMap.Entry<K, V> {
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
	private static class DefaultKeyAdapter<K> extends AbstractKeyAdapter<K> {

		@Override
		public long getKeyHashCode(Object key) {
			if (key instanceof LongHashable) {
				return ((LongHashable)key).getLongHashCode();
			} else {
				throw new IllegalArgumentException("key must implement org.logicmill.util.LongHashable");
			}
		}

		@Override
		public boolean keyMatches(Object key, K mapKey) {
			return mapKey.equals(key);
		}

	}

	@Override
	public boolean containsKey(Object key) {
		if (key == null) throw new NullPointerException();
		return hashCore.get(new MapMatchHandler(key)) != null;
	}

	@Override
	public V get(Object key) {
		if (key == null) throw new NullPointerException();
		LargeHashMap.Entry<K,V> result = hashCore.get(new MapMatchHandler(key));
		return result != null ? result.getValue() : null;
	}
	
	private class MapMatchHandler implements LargeHashCore.MatchHandler<LargeHashMap.Entry<K, V>> {

		protected final Object key;
		protected final long hashCode;
		
		private MapMatchHandler(final Object key) {
			this.key = key;
			this.hashCode = keyAdapter.getKeyHashCode(key); 
		}

		@Override
		public boolean match(
				org.logicmill.util.LargeHashMap.Entry<K, V> mappedEntry) {
			if (mappedEntry.getLongHashCode() == hashCode && 
					keyAdapter.keyMatches(key, mappedEntry.getKey())) {
				return true;
			} else {
				return false;
			}
		}
		
		@Override
		public long getLongHashCode() {
			return hashCode;
		}		
	}
	
	private class MapPutHandler extends MapMatchHandler implements PutHandler<LargeHashMap.Entry<K, V>> {
		
		protected final K key;
		protected final V value;
		
		private MapPutHandler(final K key, final V value) {
			super(key);
			this.key = key;
			this.value = value;
		}

		@Override
		public LargeHashMap.Entry<K, V> getNewEntry() {
			return new ConcurrentLargeHashMap.Entry<K, V>(key, value, hashCode);
		}
	}

	private class ValueMatchRemoveHandler extends MapMatchHandler implements LargeHashCore.RemoveHandler<LargeHashMap.Entry<K,V>> {
		
		protected final Object value;
		private ValueMatchRemoveHandler(Object key, Object value) {
			super(key);
			this.value = value;
		}
		
		@Override
		public boolean remove(LargeHashMap.Entry<K,V> mappedEntry) {
			if (mappedEntry.getValue().equals(value)) {
				return true;
			} else {
				return false;
			}
		}
	}
	
	private class KeyMatchRemoveHandler extends MapMatchHandler implements LargeHashCore.RemoveHandler<LargeHashMap.Entry<K,V>> {
		
		KeyMatchRemoveHandler(Object key) {
			super(key);
		}
		
		@Override
		public boolean remove(LargeHashMap.Entry<K,V> mappedEntry) {
			return true;
		}
	}

	private class KeyMatchReplaceHandler extends MapMatchHandler implements LargeHashCore.ReplaceHandler<LargeHashMap.Entry<K,V>> {
		protected final K key;
		protected final V newValue;
		KeyMatchReplaceHandler(K key, V newValue) {
			super(key);
			this.key = key;
			this.newValue = newValue;
		}
		@Override
		public Entry<K,V> replaceWith(LargeHashMap.Entry<K,V> matchingEntry) {
			return new Entry<K,V>(key, newValue, hashCode);
		}		
	}

	private class MapReplaceOldHandler extends MapMatchHandler implements LargeHashCore.ReplaceHandler<LargeHashMap.Entry<K,V>> {
		protected final K key;
		protected final V newValue;
		protected final V oldValue;
		MapReplaceOldHandler(K key, V oldValue, V newValue) {
			super(key);
			this.key = key;
			this.oldValue = oldValue;
			this.newValue = newValue;
		}
		@Override
		public LargeHashMap.Entry<K,V> replaceWith(LargeHashMap.Entry<K,V> mappedEntry) {
			if (mappedEntry.getValue().equals(oldValue)) {
				return new Entry<K,V>(key, newValue, hashCode);
			} else return null;
		}		
	}
	

	
	
	@Override
	public V putIfAbsent(final K key, final V value) {
		if (key == null || value == null) throw new NullPointerException();
		LargeHashMap.Entry<K, V> result = 
				hashCore.put(new MapPutHandler(key, value), false);
		return result != null ? result.getValue() : null ;
	}
	
	@Override
	public V put(K key, V value) {
		if (key == null || value == null) throw new NullPointerException();
		LargeHashMap.Entry<K, V> result = 
				hashCore.put(new MapPutHandler(key, value), true);
		return result != null ? result.getValue() : null ;
	}

	@Override
	public V remove(Object key) {
		if (key == null) throw new NullPointerException();
		LargeHashMap.Entry<K,V> result =  hashCore.remove(new KeyMatchRemoveHandler(key));
		return result != null ? result.getValue() : null ;
	}
	

	@Override
	public boolean remove(Object key, Object value) {
		if (key == null || value == null) throw new NullPointerException();
		LargeHashMap.Entry<K,V> result = hashCore.remove(new ValueMatchRemoveHandler(key, value));
		return result != null;
	}

	@Override
	public V replace(K key, V value) {
		if (key == null || value == null) throw new NullPointerException();
		LargeHashMap.Entry<K,V> result = hashCore.replace(new KeyMatchReplaceHandler(key, value));
		return result != null ? result.getValue() : null;
	}

	
	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		if (key == null || oldValue == null || newValue == null) throw new NullPointerException();
		LargeHashMap.Entry<K,V> result = hashCore.replace(new MapReplaceOldHandler(key, oldValue, newValue));
		return result != null;
	}

	@Override
	public long size() {
		return hashCore.size();
	}

	@Override
	public boolean isEmpty() {
		return hashCore.size() == 0L;
	}

	@Override
	public Iterator<LargeHashMap.Entry<K, V>> getEntryIterator() {
		return hashCore.iterator();
	}

	@Override
	public LargeHashMap.Entry<K, V> getEntry(Object key) {
		return hashCore.get(new MapMatchHandler(key));
	}
	
	@Override
	 public LargeHashSet<LargeHashMap.Entry<K,V>> entrySet()  {
		return null;
	}


}
