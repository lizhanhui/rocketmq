package org.apache.rocketmq.util.cache;

import java.util.Map;

public interface CacheEvictHandler<K, V> {
	void onEvict(Map.Entry<K, V> eldest);
}
