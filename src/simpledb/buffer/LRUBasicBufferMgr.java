/**
 * 
 */
package simpledb.buffer;

import java.util.HashMap;

import simpledb.file.Block;

/**
 * @author directxman12
 *
 */
public class LRUBasicBufferMgr extends AbstractBasicBufferMgr
{
	protected SortedQueue<TimedBuffer> _bufPool;
	protected HashMap<Block, AbstractBuffer> mapAllocated;

	/**
	 * @param numbuffs
	 */
	public LRUBasicBufferMgr(int numbuffs)
	{
		super(numbuffs);
		numAvailable = numbuffs;
		_bufPool = new SortedQueue<TimedBuffer>();
		for(int i = 0; i < numbuffs; i++) _bufPool.add(new TimedBuffer());
		mapAllocated = new HashMap<Block, AbstractBuffer>(numbuffs);
		
	}
	
	@Override
	synchronized AbstractBuffer pin(Block blk){
		AbstractBuffer buff = super.pin(blk);
		mapAllocated.put(blk, buff);
		return buff;
	}
	
	@Override
	synchronized AbstractBuffer pinNew(String filename, PageFormatter fmtr){
		AbstractBuffer buff = super.pinNew(filename, fmtr);
		mapAllocated.put(buff.block(), buff);
		return buff;
	}
	
	@Override
	protected AbstractBuffer findExistingBuffer(Block blk)
	{
		/*for (AbstractBuffer buff : _bufPool)
		{
			Block b = buff.block();
			if (b != null && b.equals(blk)) return buff;
		}
		return null;*/
		return mapAllocated.get(blk);
		
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
