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
	protected LinkedBlockingQueue<AbstractBuffer> _availBufPool;
	protected HashMap<Block, AbstractBuffer> _allocatedBufMap;
	protected int _queueSize;

	/**
	 * @param numbuffs
	 */
	public LRUBasicBufferMgr(int numbuffs)
	{
		super(numbuffs);
		_queueSize = numbuffs;
		numAvailable = numbuffs;
		_availBufPool = new LinkedBlockingQueue<AbstractBuffer>(numbuffs);
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
		    buff.assignToBlock(blk);
		}
		if (!buff.isPinned())
		{
			_availBufPool.remove(buff);
		}
		buff.pin();
		_allocatedBufMap.put(blk, buff);
		return buff;
	}
	
	
	@Override
	protected synchronized void unpin(AbstractBuffer buff)
	{
		super.unpin(buff);
		if(buff.pins < 1 /* && !_availBufPool.contains(buff) */)
		{
			_availBufPool.add(buff);
		}
	}

	@Override
	synchronized AbstractBuffer pinNew(String filename, PageFormatter fmtr)
	{
		AbstractBuffer buff = super.pinNew(filename, fmtr);
		if (buff == null) return null;
		_allocatedBufMap.put(buff.block(), buff);
		return buff;
	}
	
	@Override
	protected synchronized AbstractBuffer findExistingBuffer(Block blk)
	{
		return _allocatedBufMap.get(blk);
	}

	@Override
	protected synchronized AbstractBuffer chooseUnpinnedBuffer()
	{
		AbstractBuffer b = _availBufPool.poll();
		if (_availBufPool.contains(b))
		{
			System.out.println("WTF2!?!");
		}
		return b;
	}
	
	@Override
	public int available()
	{
		return _availBufPool.size();
	}

	@Override
	void flushAll(int txnum)
	{
      for (AbstractBuffer buff : _availBufPool) if (buff.isModifiedBy(txnum)) buff.flush();
      for (AbstractBuffer buff : _allocatedBufMap.values()) if (buff.isModifiedBy(txnum)) buff.flush();
	}
}
