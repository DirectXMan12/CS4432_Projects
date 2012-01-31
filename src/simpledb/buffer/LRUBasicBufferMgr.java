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
	protected SortedQueue<TimedBuffer> _availBufPool;
	protected HashMap<Block, AbstractBuffer> _allocatedBufMap;

	/**
	 * @param numbuffs
	 */
	public LRUBasicBufferMgr(int numbuffs)
	{
		super(numbuffs);
		numAvailable = numbuffs;
		_availBufPool = new SortedQueue<TimedBuffer>();
		_allocatedBufMap = new HashMap<Block, AbstractBuffer>(numbuffs);
		for(int i = 0; i < numbuffs; i++)
		{
			TimedBuffer b = new TimedBuffer();
			b.setAvailSet(_availBufPool);
			b.setPinnedMap(_allocatedBufMap);
			_availBufPool.add(new TimedBuffer());
		}
		
	}
	
	@Override
	synchronized AbstractBuffer pin(Block blk)
	{
		AbstractBuffer buff = super.pin(blk);
		_allocatedBufMap.put(blk, buff);
		return buff;
	}
	
	@Override
	synchronized AbstractBuffer pinNew(String filename, PageFormatter fmtr)
	{
		AbstractBuffer buff = super.pinNew(filename, fmtr);
		_allocatedBufMap.put(buff.block(), buff);
		return buff;
	}
	
	@Override
	protected AbstractBuffer findExistingBuffer(Block blk)
	{
		return _allocatedBufMap.get(blk);
	}

	@Override
	protected AbstractBuffer chooseUnpinnedBuffer()
	{
		return _availBufPool.poll();
	}

	@Override
	void flushAll(int txnum)
	{
      for (AbstractBuffer buff : _availBufPool) if (buff.isModifiedBy(txnum)) buff.flush();
	}
}
