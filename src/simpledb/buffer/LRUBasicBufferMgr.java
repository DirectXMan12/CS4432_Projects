/**
 * 
 */
package simpledb.buffer;

import java.util.Comparator;
import java.util.PriorityQueue;

import simpledb.file.Block;

/**
 * @author directxman12
 *
 */
public class LRUBasicBufferMgr extends AbstractBasicBufferMgr
{
	protected PriorityQueue<TimedBuffer> _bufPool;

	/**
	 * @param numbuffs
	 */
	public LRUBasicBufferMgr(int numbuffs)
	{
		super(numbuffs);
		numAvailable = numbuffs;
		_bufPool = new PriorityQueue<TimedBuffer>(numbuffs);
		
	}
	
	@Override
	protected Buffer findExistingBuffer(Block blk)
	{
		// TODO: make a more efficient version of this
		TimedBuffer[] bufferpool = (TimedBuffer[]) _bufPool.toArray();
		for (Buffer buff : bufferpool)
		{
			Block b = buff.block();
			if (b != null && b.equals(blk))
			return buff;
		}
		return null;
	}

	@Override
	protected Buffer chooseUnpinnedBuffer()
	{
		return _bufPool.peek();
	}

	@Override
	void flushAll(int txnum)
	{
      for (Buffer buff : _bufPool)
          if (buff.isModifiedBy(txnum))
          buff.flush();
	}
}
