package com.colobu.zkrecipe.queue;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

import com.google.common.collect.ImmutableList;

/**
 * <p>An implementation of the Distributed Queue ZK recipe which implement BlockingQueue interface and QueueConsumer interface. Items put into the queue
 * are guaranteed to be ordered (by means of ZK's PERSISTENT_SEQUENTIAL node) and some items are consumed by this queue</p>
 *
 * <p>It maybe seems strange: many items maybe are added by this queue but size() return less than added. Remember it is a distributed queue. 
 * <em>Add</em> means you add items into the queue for all nodes, but <em>retrieve</em> means you can only get items from the piece of queue consumed by this object.
 * And <em>size</em> only indicates its local queue size.
 * </p>
 * 
 * @author <a href="mailto:smallnest@gmail.com">smallnest</a>
 * @param <E> the type of elements held in this collection
 */
public class AkinDistributedBlockingQueue<E> implements BlockingQueue<E>, QueueConsumer<E> {
	private final ConnectionStateListener connectionStateListener;
	private final BlockingQueue<E> localInternalQueue;
	private DistributedQueue<E> distributedQueue;

	private final static long DEFAULT_WAITTIME = 3000;

	public AkinDistributedBlockingQueue(DistributedQueue<E> distributedQueue, ConnectionStateListener connectionStateListener) {
		this(distributedQueue, connectionStateListener, new LinkedBlockingQueue<E>());
	}

	public AkinDistributedBlockingQueue(DistributedQueue<E> distributedQueue,ConnectionStateListener connectionStateListener, int capacity) {
		this(distributedQueue, connectionStateListener, new ArrayBlockingQueue<E>(capacity));
	}

	public AkinDistributedBlockingQueue(DistributedQueue<E> distributedQueue, ConnectionStateListener connectionStateListener, BlockingQueue<E> queue) {
		this.connectionStateListener = connectionStateListener;
		this.localInternalQueue = queue;
		this.setDistributedQueue(distributedQueue);
	}

	@Override
	public void consumeMessage(E message) throws Exception {
		localInternalQueue.add(message);
	}

	@Override
	public E remove() {
		return localInternalQueue.remove();
	}

	@Override
	public E poll() {
		return localInternalQueue.poll();
	}

	@Override
	public E element() {
		return localInternalQueue.element();
	}

	@Override
	public E peek() {
		return localInternalQueue.peek();
	}

	@Override
	public int size() {
		return localInternalQueue.size();
	}

	@Override
	public boolean isEmpty() {
		return localInternalQueue.isEmpty();
	}

	@Override
	public Iterator<E> iterator() {
		return localInternalQueue.iterator();
	}

	@Override
	public Object[] toArray() {
		return localInternalQueue.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return localInternalQueue.toArray(a);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return localInternalQueue.containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		return localInternalQueue.addAll(c);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return localInternalQueue.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {

		return localInternalQueue.retainAll(c);
	}

	@Override
	public void clear() {
		localInternalQueue.clear();
	}

	@Override
	public boolean add(E e) {
		try {
			getDistributedQueue().put(e);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		return true;
	}

	@Override
	public boolean offer(E e) {
		return add(e);
	}

	@Override
	public void put(E e) throws InterruptedException {
		try {
			getDistributedQueue().put(e);
			if (!getDistributedQueue().flushPuts(DEFAULT_WAITTIME, TimeUnit.MILLISECONDS))
				throw new RuntimeException("timeout");
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {

		try {
			return getDistributedQueue().put(e, (int) timeout, unit);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public E take() throws InterruptedException {

		return localInternalQueue.take();
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {

		return localInternalQueue.poll(timeout, unit);
	}

	@Override
	public int remainingCapacity() {

		return localInternalQueue.remainingCapacity();
	}

	@Override
	public boolean remove(Object o) {
		return localInternalQueue.remove(o);
	}

	@Override
	public boolean contains(Object o) {

		return localInternalQueue.contains(o);
	}

	@Override
	public int drainTo(Collection<? super E> c) {

		return localInternalQueue.drainTo(c);
	}

	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {

		return localInternalQueue.drainTo(c, maxElements);
	}

	public List<E> getElements() {
		return ImmutableList.copyOf(localInternalQueue);
	}

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		connectionStateListener.stateChanged(client, newState);
	}

	public DistributedQueue<E> getDistributedQueue() {
		return distributedQueue;
	}

	public void setDistributedQueue(DistributedQueue<E> distributedQueue) {
		this.distributedQueue = distributedQueue;
	}

}
