package com.colobu.zkrecipe.queue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.queue.SimpleDistributedQueue;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

public class SimpleDistributedQueueExample {
	private static final String PATH = "/example/queue";

	public static void main(String[] args) throws Exception {
		TestingServer server = new TestingServer();
		CuratorFramework client = null;
		SimpleDistributedQueue queue = null;
		try {
			client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
			client.getCuratorListenable().addListener(new CuratorListener() {
				@Override
				public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
					System.out.println("CuratorEvent: " + event.getType().name());
				}
			});

			client.start();
			queue = new SimpleDistributedQueue(client, PATH);
			for (int i = 0; i < 10; i++) {
				queue.offer(("test-" + i).getBytes());
			}
			
			for (int i = 0; i < 10; i++) {
				System.out.println("took: " + new String(queue.take()));
			}

			Thread.sleep(20000);

		} catch (Exception ex) {

		} finally {
			CloseableUtils.closeQuietly(client);
			CloseableUtils.closeQuietly(server);
		}
	}
}
