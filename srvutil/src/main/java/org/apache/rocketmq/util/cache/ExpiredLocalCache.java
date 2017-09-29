package org.apache.rocketmq.util.cache;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;

import java.util.ArrayList;
import java.util.List;


public class ExpiredLocalCache<K, T> {
	private ConcurrentLinkedHashMap<K, CacheObject<T>> cache;
	private EvictionListener<K, CacheObject<T>> listener;
	public ExpiredLocalCache(int size) {
		cache=new ConcurrentLinkedHashMap.Builder<K, CacheObject<T>>().maximumWeightedCapacity(size).build();
	}	
	public ExpiredLocalCache(int size,String name) {
		cache=new ConcurrentLinkedHashMap.Builder<K, CacheObject<T>>().maximumWeightedCapacity(size).build();
	}	
	public ExpiredLocalCache(int size, boolean memoryMeter, EvictionListener<K, CacheObject<T>> listener) {
		this.listener=listener;
		cache=new ConcurrentLinkedHashMap.Builder<K, CacheObject<T>>().listener(listener).maximumWeightedCapacity(size).build();
	}		
	public static void main(String[] args) {
		ExpiredLocalCache<String, List<String>> cache=new ExpiredLocalCache<String, List<String>>(100000);
		List<String> list=new ArrayList<String>();
		list.add("sdfsdf");
		list.add("sdfasdfasdfasdfsdf");
		cache.put("sdfsdf", list, 10000);
		System.out.println(cache.getCache().weightedSize());
		System.out.println(cache.getCache().capacity());
		
		list=new ArrayList<String>();
		list.add("sdfsdf");
		list.add("sdfasdfasdfasdfsdf");
		cache.put("sdfsdf1", list, 10000);
		cache.put("sdfsdf2", list, 10000);

		System.out.println(cache.getCache().weightedSize());
		System.out.println(cache.getCache().capacity());
	}

	public T get(K key) {
		CacheObject<T> object = cache.get(key);
		if (object == null) {
			return null;
		}
		T ret = object.getTarget();
		if (ret == null) {
			this.delete(key);
		}
		return ret;
	}

	public T put(K key, T v, long exp) {
		CacheObject<T> value = new CacheObject<T>(exp, v);
		CacheObject<T> old = cache.put(key, value);
		if (old == null) {
			return null;
		} else {
			return old.getTarget();
		}
	}
	public T putIfAbsent(K key, T v, long exp) {
		CacheObject<T> value = new CacheObject<T>(exp, v);
		CacheObject<T> old = cache.putIfAbsent(key, value);
		if (old == null) {
			return null;
		} else {
			return old.getTarget();
		}
	}
	public T delete(K key) {
		CacheObject<T> object = cache.remove(key);
		if (object == null) {
			return null;
		}
		T ret = object.getTarget();
		return ret;
	}
	public ConcurrentLinkedHashMap<K, CacheObject<T>> getCache() {
		return cache;
	}

}
