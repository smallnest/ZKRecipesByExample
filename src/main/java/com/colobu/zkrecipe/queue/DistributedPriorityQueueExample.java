package com.colobu.zkrecipe.queue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.queue.DistributedPriorityQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

public class DistributedPriorityQueueExample {
	private static final String PATH = "/example/queue";

	public static void main(String[] args) throws Exception {
		TestingServer server = new TestingServer();
		CuratorFramework client = null;
		DistributedPriorityQueue<String> queue = null;
		try {
			client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
			client.getCuratorListenable().addListener(new CuratorListener() {
				@Override
				public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
					System.out.println("CuratorEvent: " + event.getType().name());
				}
			});

			client.start();
			QueueConsumer<String> consumer = createQueueConsumer();
			QueueBuilder<String> builder = QueueBuilder.builder(client, consumer, createQueueSerializer(), PATH);
			queue = builder.buildPriorityQueue(0);
			queue.start();
			
			for (int i = 0; i < 10; i++) {
				int priority = (int)(Math.random() * 100);
				System.out.println("test-" + i + " priority:" + priority);
				queue.put("test-" + i, priority);
				Thread.sleep((long)(50 * Math.random()));
			}
			
			Thread.sleep(20000);
			
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

	private static QueueConsumer<String> createQueueConsumer() {

		return new QueueConsumer<String>(){

			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				System.out.println("connection new state: " + newState.name());
			}

			@Override
			public void consumeMessage(String message) throws Exception {
				System.out.println("consume one message: " + message);				
			}
			
		};
	}
}
