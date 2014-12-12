package com.colobu.zkrecipe.lock;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

public class InterProcessMultiLockExample {
	private static final String PATH1 = "/examples/locks1";
	private static final String PATH2 = "/examples/locks2";
	
	public static void main(String[] args) throws Exception {
		FakeLimitedResource resource = new FakeLimitedResource();
		try (TestingServer server = new TestingServer()) {
			CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
			client.start();
			
			InterProcessLock lock1 = new InterProcessMutex(client, PATH1);
			InterProcessLock lock2 = new InterProcessSemaphoreMutex(client, PATH2);
			
			InterProcessMultiLock lock = new InterProcessMultiLock(Arrays.asList(lock1, lock2));

			if (!lock.acquire(10, TimeUnit.SECONDS)) {
				throw new IllegalStateException("could not acquire the lock");
			}
			System.out.println("has the lock");
			
			System.out.println("has the lock1: " + lock1.isAcquiredInThisProcess());
			System.out.println("has the lock2: " + lock2.isAcquiredInThisProcess());
			
			try {			
				resource.use(); //access resource exclusively
			} finally {
				System.out.println("releasing the lock");
				lock.release(); // always release the lock in a finally block
			}
			System.out.println("has the lock1: " + lock1.isAcquiredInThisProcess());
			System.out.println("has the lock2: " + lock2.isAcquiredInThisProcess());
		}
	}

}
