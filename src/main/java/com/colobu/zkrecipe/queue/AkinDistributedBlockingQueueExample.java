package com.colobu.zkrecipe.queue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

public class AkinDistributedBlockingQueueExample {
	private static final String PATH = "/example/queue";

	public static void main(String[] args) throws Exception {
		TestingServer server = new TestingServer();
		CuratorFramework client = null;
		DistributedQueue<String> queue = null;
		try {
			client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
			client.getCuratorListenable().addListener(new CuratorListener() {
				@Override
				public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
					System.out.println("CuratorEvent: " + event.getType().name());
				}
			});

			client.start();
			AkinDistributedBlockingQueue<String> consumerQueue = new AkinDistributedBlockingQueue<String>(null, new ConnectionStateListener() {
				
				@Override
				public void stateChanged(CuratorFramework client, ConnectionState newState) {
					
				}
			});
			QueueBuilder<String> builder = QueueBuilder.builder(client, consumerQueue, createQueueSerializer(), PATH);
			queue = builder.buildQueue();
			consumerQueue.setDistributedQueue(queue);
			queue.start();
			
			for (int i = 0; i < 10; i++) {
				queue.put(" test-" + i);
				Thread.sleep((long)(3 * Math.random()));
			}
			
						
			Thread.sleep(20000);
			
			for (Object object : consumerQueue) {
				System.out.println(consumerQueue.poll());
			}
			
		} catch (Exception ex) {

		} finally {
			CloseableUtils.closeQuietly(queue);
			CloseableUtils.closeQuietly(client);
			CloseableUtils.closeQuietly(server);
		}
	}

	private static QueueSerializer<String> createQueueSerializer() {
		return new QueueSerializer<String>(){

			@Override
			public byte[] serialize(String item) {
				return item.getBytes();
			}

			@Override
			public String deserialize(byte[] bytes) {
				return new String(bytes);
			}
			
		};
	}
}
