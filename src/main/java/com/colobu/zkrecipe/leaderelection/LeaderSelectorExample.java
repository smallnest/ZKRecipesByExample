package com.colobu.zkrecipe.leaderelection;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import com.google.common.collect.Lists;

public class LeaderSelectorExample {
	private static final int CLIENT_QTY = 10;
	private static final String PATH = "/examples/leader";

	public static void main(String[] args) throws Exception {
		List<CuratorFramework> clients = Lists.newArrayList();
		List<ExampleClient> examples = Lists.newArrayList();
		TestingServer server = new TestingServer();
		try {
			for (int i = 0; i < CLIENT_QTY; ++i) {
				CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
				clients.add(client);
				ExampleClient example = new ExampleClient(client, PATH, "Client #" + i);
				examples.add(example);
				client.start();
				example.start();
			}
			
			System.out.println("Press enter/return to quit\n");
			new BufferedReader(new InputStreamReader(System.in)).readLine();
		} finally {
			System.out.println("Shutting down...");
			for (ExampleClient exampleClient : examples) {
				CloseableUtils.closeQuietly(exampleClient);
			}
			for (CuratorFramework client : clients) {
				CloseableUtils.closeQuietly(client);
			}
			CloseableUtils.closeQuietly(server);
		}
	}
}
