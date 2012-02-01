/**
 * 
 */
package simpledb.buffer;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import simpledb.file.Block;

/**
 * @author directxman12
 *
 */
public class LRUBasicBufferMgr extends AbstractBasicBufferMgr
{
	protected ArrayBlockingQueue<AbstractBuffer> _availBufPool;
	protected HashMap<Block, AbstractBuffer> _allocatedBufMap;

	/**
	 * @param numbuffs
	 */
	public LRUBasicBufferMgr(int numbuffs)
	{
		super(numbuffs);
		numAvailable = numbuffs;
		_availBufPool = new ArrayBlockingQueue<AbstractBuffer>(numbuffs);
		_allocatedBufMap = new HashMap<Block, AbstractBuffer>(numbuffs);
		for(int i = 0; i < numbuffs; i++) _availBufPool.add(new Buffer());
		
	}
	
	@Override
	synchronized AbstractBuffer pin(Block blk)
	{
		AbstractBuffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
		    if (buff == null) return null;
		    _allocatedBufMap.remove(buff.block());
			// _availBufPool.remove(buff); // not needed b/c poll called on choose unpinned buffer
		    buff.assignToBlock(blk);
		}
		if (!buff.isPinned()) numAvailable--;
		buff.pin();
		_allocatedBufMap.put(blk, buff);
		return buff;
	}
	
	
	@Override
	protected synchronized void unpin(AbstractBuffer buff)
	{
		super.unpin(buff);
		System.out.println(_availBufPool.size());
		_availBufPool.add(buff);
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
      for (AbstractBuffer buff : _allocatedBufMap.values()) if (buff.isModifiedBy(txnum)) buff.flush();
	}
}
