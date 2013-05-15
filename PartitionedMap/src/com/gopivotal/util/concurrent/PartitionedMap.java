package com.gopivotal.util.concurrent;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link PartitionedMap} is a map which provides fast read-only access to its
 * state by maintaining a read-only view of the state and queuing update
 * operations and processing them asynchronously to replace the read-only view.
 * 
 */
public class PartitionedMap<K, V> implements Map<K, V> {

	private enum OpCode {
		PUT, REMOVE, PUTALL, CLEAR
	};

	private class Op {
		private OpCode opCode;
		private K key;
		private V value;
		private Object parm;
		private Future<V> future;

		public Op(OpCode opCode, K key, V value, Object parm, Future<V> future) {
			this.opCode = opCode;
			this.key = key;
			this.value = value;
			this.parm = parm;
			this.future = future;
		}

		public OpCode getOpCode() {
			return this.opCode;
		}

		public K getKey() {
			return this.key;
		}

		public V getValue() {
			return this.value;
		}

		public Object getParm() {
			return this.parm;
		}

		public Future<V> getFuture() {
			return this.future;
		}
	}

	public Op createPut(K key, V value) {
		Future<V> ffuture = createFuture();
		return new Op(OpCode.PUT, key, value, null, ffuture);
	}

	private Future<V> createFuture() {
		Future<V> f = new Future<V>() {

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean isCancelled() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public boolean isDone() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public V get() throws InterruptedException, ExecutionException {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public V get(long timeout, TimeUnit unit)
					throws InterruptedException, ExecutionException,
					TimeoutException {
				// TODO Auto-generated method stub
				return null;
			}
		};
		return f;
	}

	public Op createRemove(Object key) {
		Future<V> future = createFuture();
		return new Op(OpCode.REMOVE, null, null, key, future);
	}

	public Op createPutAll(Map<? extends K, ? extends V> m) {
		Future<V> future = createFuture();
		return new Op(OpCode.REMOVE, null, null, m, future);
	}

	public Op createClear() {
		Future<V> future = createFuture();
		return new Op(OpCode.CLEAR, null, null, null, future);
	}

	private ConcurrentLinkedQueue<Op> queue = new ConcurrentLinkedQueue<Op>();

	private AtomicReference<Map<K, V>> viewRef;

	private ConcurrentMap<K, V> state;

	public PartitionedMap() {
		viewRef = new AtomicReference<Map<K, V>>(new Hashtable<K, V>());
		state = new ConcurrentHashMap<K, V>();
	}

	private Map<K, V> createNewView(Map<K, V> oldView) {
		return new Hashtable<K, V>(oldView);
	}

	@Override
	public int size() {
		return viewRef.get().size();
	}

	@Override
	public boolean isEmpty() {
		return viewRef.get().isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return viewRef.get().containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return viewRef.get().containsValue(value);
	}

	@Override
	public V get(Object key) {
		return viewRef.get().get(key);
	}

	@Override
	public V put(K key, V value) {
		while (true) {
			Future<V> f = putFuture(key, value);
			try {
				return f.get();
			} catch (Exception e) {
			}
		}
	}

	public Future<V> putFuture(K key, V value) {
		Op op = createPut(key, value);
		this.queue.add(op);
		return op.getFuture();
	}

	@Override
	public V remove(Object key) {
		while (true) {
			Future<V> f = removeFuture(key);
			try {
				return f.get();
			} catch (Exception e) {
			}
		}
	}

	public Future<V> removeFuture(Object key) {
		Op op = createRemove(key);
		this.queue.add(op);
		return op.getFuture();
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		Op op = createPutAll(m);
		this.queue.add(op);
	}

	@Override
	public void clear() {
		Op op = createClear();
		this.queue.add(op);
	}

	/*
	 * Bulk operations which return unmodifiable values.
	 * 
	 * The alternative would be to store an unmodifiable set as the view, but
	 * this would complicate update as the modifiable form would need recording
	 * too.
	 */

	@Override
	public Set<K> keySet() {
		return Collections.unmodifiableSet(viewRef.get().keySet());
	}

	@Override
	public Collection<V> values() {
		return Collections.unmodifiableCollection(viewRef.get().values());
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return Collections.unmodifiableSet(viewRef.get().entrySet());
	}

	/*
	 * Queue processor interface.
	 */

	@SuppressWarnings("unchecked")
	public void processQueue() {
		Set<Future<V>> futures = new HashSet<Future<V>>();
		Op op = queue.poll();
		while (op != null) {
			switch (op.getOpCode()) {
			case PUT:
				state.put(op.getKey(), op.getValue());
				break;
			case REMOVE:
				state.remove(op.getParm());
				break;
			case PUTALL:
				state.putAll((Map<? extends K, ? extends V>) op.getParm());
				break;
			case CLEAR:
				state.clear();
				break;
			default:
				// Ignore bad queue item
				break;
			}
			Future<V> future = op.getFuture();
			futures.add(future);
			op = queue.poll();
		}
		this.viewRef.set(createNewView(state));
	}

}
