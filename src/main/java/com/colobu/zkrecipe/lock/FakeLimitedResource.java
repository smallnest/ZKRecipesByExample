package com.colobu.zkrecipe.lock;

import java.util.concurrent.atomic.AtomicBoolean;

public class FakeLimitedResource {
	private final AtomicBoolean inUse = new AtomicBoolean(false);

	public void use() throws InterruptedException {
		// 真实环境中我们会在这里访问/维护一个共享的资源
		//这个例子在使用锁的情况下不会非法并发异常IllegalStateException
		//但是在无锁的情况由于sleep了一段时间，很容易抛出异常
		if (!inUse.compareAndSet(false, true)) { 
			throw new IllegalStateException("Needs to be used by one client at a time");
		}
		try {
			Thread.sleep((long) (3 * Math.random()));
		} finally {
			inUse.set(false);
		}
	}
}