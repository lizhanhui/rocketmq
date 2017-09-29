package org.apache.rocketmq.util.cache;


public class CacheObject<T> {
	private T target;
	private long bornTime=System.currentTimeMillis();
	private long exp;
	public CacheObject(long exp,T target){
		this.exp = exp;
		this.target = target;
	}
	public T getTarget() {
		if (System.currentTimeMillis()-bornTime>exp) {
			return null;
		}
		return target;
	}
}
