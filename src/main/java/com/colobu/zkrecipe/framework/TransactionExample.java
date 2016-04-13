package com.colobu.zkrecipe.framework;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;

import java.util.Collection;
import java.util.List;

import static com.colobu.zkrecipe.framework.CreateClientExample.createSimple;

public class TransactionExample {

	public static void main(String[] args) throws Exception {
		TestingServer server = new TestingServer();
		CuratorFramework client = null;
		try {
			client = createSimple(server.getConnectString());
			client.start();

			ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(),"/a");
			ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(),"/another/path");
			ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(),"/yet/another/path");


			transaction(client);

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			CloseableUtils.closeQuietly(client);
			CloseableUtils.closeQuietly(server);
		}
	}

	public static Collection<CuratorTransactionResult> transaction(CuratorFramework client) throws Exception {
//		// this example shows how to use ZooKeeper's new transactions
//		Collection<CuratorTransactionResult> results = client.inTransaction().create().forPath("/a/path", "some data".getBytes())
//				.and().setData().forPath("/another/path", "other data".getBytes())
//				.and().delete().forPath("/yet/another/path")
//				.and().commit(); // IMPORTANT!

		//inTransaction is deprecated. use transaction() instead
		List<CuratorTransactionResult>  results = client.transaction().forOperations(
				client.transactionOp().create().forPath("/a/path", "some data".getBytes()),
				client.transactionOp().setData().forPath("/another/path", "other data".getBytes()),
				client.transactionOp().delete().forPath("/yet/another/path"));
		// called
		for (CuratorTransactionResult result : results) {
			System.out.println(result.getForPath() + " - " + result.getType());
		}
		return results;
	}
}
