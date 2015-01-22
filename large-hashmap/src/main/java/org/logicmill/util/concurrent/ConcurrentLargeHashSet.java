package org.logicmill.util.concurrent;

import java.util.Iterator;

import org.logicmill.util.LargeHashCore;
import org.logicmill.util.LargeHashSet;
import org.logicmill.util.TypeAdapter;
import org.logicmill.util.LongHashable;
import org.logicmill.util.LargeHashCore.PutHandler;

public class ConcurrentLargeHashSet<E> implements LargeHashSet<E> {

	private final LargeHashCore<E> hashCore;
	private final TypeAdapter<E> typeAdapter; // only used locally for hashCode, should work for key or map adapter
	
	private static class DefaultTypeAdapter<E> implements TypeAdapter<E> {

		@Override
		public long getLongHashCode(Object obj) {
			if (obj instanceof LongHashable) {
				return ((LongHashable)obj).getLongHashCode();
			} else {
				throw new IllegalArgumentException("entry type must implement org.logicmill.util.LongHashable");
			}
		}

		@Override
		public boolean matches(Object obj, E entry) {
			return entry.equals(obj);
		}
		
	}
	
	/**
	 * @param segSize
	 * @param initSegCount
	 * @param loadFactorThreshold
	 * @param keyAdapter
	 */
	public ConcurrentLargeHashSet(int segSize, int initSegCount,
			float loadFactorThreshold, TypeAdapter<E> adapter) {
		hashCore = new ConcurrentLargeHashCore<E>(
				segSize, initSegCount, loadFactorThreshold, adapter);
		typeAdapter = adapter;
	}
	
	/**
	 * @param segSize
	 * @param initSegCount
	 * @param loadFactorThreshold
	 */
	public ConcurrentLargeHashSet(int segSize, int initSegCount, float loadFactorThreshold) {
		this(segSize, initSegCount, loadFactorThreshold, new DefaultTypeAdapter());
		
	}

	
	private class SetMatchHandler implements LargeHashCore.MatchHandler<E> {

		protected final Object entry;
		protected final long hashCode;
		
		private SetMatchHandler(final Object obj) {
			this.entry = obj;
			this.hashCode = typeAdapter.getLongHashCode(obj); 
		}

		@Override
		public boolean match(E mappedEntry) {
			if (typeAdapter.getLongHashCode(mappedEntry) == hashCode && 
					typeAdapter.matches(entry, mappedEntry)) {
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
	
	private class SetPutHandler extends SetMatchHandler implements PutHandler<E> {
		
		protected final E entry;
		
		private SetPutHandler(E entry) {
			super(entry);
			this.entry = entry;
		}

		@Override
		public E getNewEntry() {
			return entry;
		}
	}
	
	private class SetRemoveHandler extends SetMatchHandler implements LargeHashCore.RemoveHandler<E> {
		
		SetRemoveHandler(Object entry) {
			super(entry);
		}
		
		@Override
		public boolean remove(E mappedEntry) {
			return true;
		}
	}
	
	@Override
	public Iterator<E> iterator() {
		return hashCore.iterator();
	}

	@Override
	public boolean add(E entry) {
		if (entry == null) throw new NullPointerException();
		E result = hashCore.put(new SetPutHandler(entry), false);
		return result == null;
	}

	@Override
	public boolean contains(Object entry) {
		return get(entry) != null;
	}

	@Override
	public boolean isEmpty() {
		return size() == 0L;
	}

	@Override
	public E remove(Object entry) {
		if (entry == null) throw new NullPointerException();
		return hashCore.remove(new SetRemoveHandler(entry));
	}

	@Override
	public long size() {
		return hashCore.size();
	}

	@Override
	public E get(Object entry) {
		return hashCore.get(new SetMatchHandler(entry));
	}

}
