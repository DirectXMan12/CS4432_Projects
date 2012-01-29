/**
 * 
 */
package simpledb.buffer;

import simpledb.file.Block;

/**
 * @author directxman12
 *
 */
public class LRUBasicBufferMgr extends AbstractBasicBufferMgr
{
	protected SortedQueue<TimedBuffer> _bufPool;

	/**
	 * @param numbuffs
	 */
	public LRUBasicBufferMgr(int numbuffs)
	{
		super(numbuffs);
		numAvailable = numbuffs;
		_bufPool = new SortedQueue<TimedBuffer>();
		for(int i = 0; i < numbuffs; i++) _bufPool.add(new TimedBuffer());
		
	}
	
	@Override
	protected AbstractBuffer findExistingBuffer(Block blk)
	{
		for (AbstractBuffer buff : _bufPool)
		{
			Block b = buff.block();
			if (b != null && b.equals(blk)) return buff;
		}
		return null;
	}

	@Override
	protected AbstractBuffer chooseUnpinnedBuffer()
	{
		return _bufPool.peek();
	}

	@Override
	void flushAll(int txnum)
	{
      for (AbstractBuffer buff : _bufPool) if (buff.isModifiedBy(txnum)) buff.flush();
	}
}
