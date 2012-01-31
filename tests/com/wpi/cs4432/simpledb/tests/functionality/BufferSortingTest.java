/**
 * 
 */
package com.wpi.cs4432.simpledb.tests.functionality;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import simpledb.buffer.SortedQueue;
import simpledb.buffer.TimedBuffer;
import simpledb.file.Block;

/**
 * @author directxman12
 *
 */
public class BufferSortingTest
{

	@Test
	public void testBufferSorting() throws InterruptedException
	{
		SortedQueue<TimedBufferShim> buffQueue = new SortedQueue<TimedBufferShim>();
		
		Block blk1 = new Block("/home/directxman12/studentdb/tblcat.tbl", 27);
		Block blk2 = new Block("/home/directxman12/studentdb/tblcat.tbl", 38);
		Block blk3 = new Block("/home/directxman12/studentdb/tblcat.tbl", 41);
		
		TimedBufferShim buff1 = new TimedBufferShim(1);
		buff1.assignToBlock(blk1);
		Thread.sleep(10);
		TimedBufferShim buff3 = new TimedBufferShim(2);
		buff3.assignToBlock(blk3);
		Thread.sleep(10);
		TimedBufferShim buff2 = new TimedBufferShim(3);
		buff2.assignToBlock(blk2);
		
		
		buffQueue.add(buff1);
		buffQueue.add(buff2);
		buffQueue.add(buff3);
		
		assertArrayEquals(new TimedBufferShim[] {buff1, buff3, buff2}, buffQueue.toArray(new TimedBufferShim[] {}));
	}
	
	@Test
	public void testBufferOrderChange() throws InterruptedException
	{
		SortedQueue<TimedBuffer> buffQueue = new SortedQueue<TimedBuffer>();
		
		Block blk1 = new Block("/home/directxman12/studentdb/tblcat.tbl", 27);
		Block blk2 = new Block("/home/directxman12/studentdb/tblcat.tbl", 38);
		Block blk3 = new Block("/home/directxman12/studentdb/tblcat.tbl", 41);
		
		TimedBufferShim buff1 = new TimedBufferShim(1);
		buff1.assignToBlock(blk1);
		Thread.sleep(10);
		TimedBufferShim buff3 = new TimedBufferShim(10);
		buff3.assignToBlock(blk3);
		Thread.sleep(10);
		TimedBufferShim buff2 = new TimedBufferShim(100);
		buff2.assignToBlock(blk2);
		
		
		buffQueue.add(buff1);
		buff1.setAvailSet(buffQueue);
		buffQueue.add(buff2);
		buff2.setAvailSet(buffQueue);
		buffQueue.add(buff3);
		buff3.setAvailSet(buffQueue);
		
		assertArrayEquals(new TimedBufferShim[] {buff1, buff3, buff2}, buffQueue.toArray(new TimedBufferShim[] {}));
		
		buff1.triggerUsed();
		Thread.sleep(10);
		
		assertArrayEquals(new TimedBufferShim[] {buff3, buff2, buff1}, buffQueue.toArray(new TimedBufferShim[] {}));
	}
	
	@Test
	public void testSortedQueueInsertionOrder()
	{
		SortedQueue<Integer> squeue = new SortedQueue<Integer>();
		squeue.add(10);
		squeue.add(12);
		assertArrayEquals(new Integer[] {10,12}, squeue.toArray(new Integer[]{}));
		squeue.add(11);
		assertArrayEquals(new Integer[] {10,11,12}, squeue.toArray(new Integer[]{}));
	}
	
	@Test
	public void testBasicSortedQueueOrder()
	{
		SortedQueue<Integer> squeue = new SortedQueue<Integer>();
		squeue.add(10);
		squeue.add(12);
		squeue.add(1);
		squeue.add(4);
		squeue.add(47);
		assertArrayEquals(new Integer[] {1,4,10,12,47}, squeue.toArray(new Integer[]{}));
	}

}
