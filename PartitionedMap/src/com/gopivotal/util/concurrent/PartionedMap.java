package com.gopivotal.util.concurrent;

import java.util.Map;

public interface PartionedMap<V, K> extends Map<K, V> {

	PartionedMapViewer<K, V> getViewer();

	V put(K key, V value);

	V remove(Object key);

	void putAll(Map<? extends K, ? extends V> m);

	void clear();

}
