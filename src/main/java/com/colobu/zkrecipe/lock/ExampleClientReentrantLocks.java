package com.colobu.zkrecipe.lock;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

public class ExampleClientReentrantLocks {
	private final InterProcessMutex lock;
	private final FakeLimitedResource resource;
	private final String clientName;

	public ExampleClientReentrantLocks(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
		this.resource = resource;
		this.clientName = clientName;
		lock = new InterProcessMutex(client, lockPath);
	}

	public void doWork(long time, TimeUnit unit) throws Exception {
		if (!lock.acquire(time, unit)) {
			throw new IllegalStateException(clientName + " could not acquire the lock");
		}
		System.out.println(clientName + " has the lock");
		if (!lock.acquire(time, unit)) {
			throw new IllegalStateException(clientName + " could not acquire the lock");
		}
		System.out.println(clientName + " has the lock again");
		
		try {			
			resource.use(); //access resource exclusively
		} finally {
			System.out.println(clientName + " releasing the lock");
			lock.release(); // always release the lock in a finally block
			lock.release(); // always release the lock in a finally block
		}
	}
}