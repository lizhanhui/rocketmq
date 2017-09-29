package org.apache.rocketmq.util.cache;

import java.util.LinkedHashMap;
import java.util.Map;




public class LocalCache<K, V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 1606231700062718297L;

    private static final int DEFAULT_CACHE_SIZE = 1000;

    private int cacheSize = DEFAULT_CACHE_SIZE;
    private CacheEvictHandler<K, V> handler;

    /**
     * The default initial capacity - MUST be a power of two.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16


    /**
     * The load factor used when none specified in constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    public LocalCache(int cacheSize,boolean isLru,CacheEvictHandler<K, V> handler) {
        super(DEFAULT_INITIAL_CAPACITY,DEFAULT_LOAD_FACTOR,isLru);
    	this.cacheSize = cacheSize;
    	this.handler=handler;
    }


    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    	boolean result= this.size() > cacheSize;
    	if (result&&handler!=null) {
			handler.onEvict(eldest);
		}
        return result;
    }
    
}
