/**
 * 
 */
package simpledb.buffer;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import simpledb.file.Block;

/**
 * @author jeffnamias
 *
 */
public class MRUBasicBufferMgr extends AbstractBasicBufferMgr
{
	protected LinkedBlockingDeque<Buffer> _availBufPool; // preforms differently if linked vs array
	protected HashMap<Block, Buffer> _allocatedBufMap;
	protected int _queueSize;

	/**
	 * @param numbuffs
	 */
	public MRUBasicBufferMgr(int numbuffs)
	{
		super(numbuffs);
		_queueSize = numbuffs;
		numAvailable = numbuffs;
		_availBufPool = new LinkedBlockingDeque<Buffer>(numbuffs);
		_allocatedBufMap = new HashMap<Block, Buffer>(numbuffs);
		for(int i = 0; i < numbuffs; i++) _availBufPool.add(new Buffer());
		
	}
	
	@Override
	synchronized Buffer pin(Block blk)
	{
		// for inline docs, see the LRU pin method -- this once works the same way
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
		    if (buff == null) return null;
		    _allocatedBufMap.remove(buff.block());
		    buff.assignToBlock(blk);
		    _ioCount++;
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
	protected synchronized void unpin(Buffer buff)
	{
		super.unpin(buff);
		if(buff.pins < 1 /* && !_availBufPool.contains(buff) */)
		{
			_availBufPool.addFirst(buff); // adds to the head, not the tail like LRU
		}
	}

	@Override
	synchronized Buffer pinNew(String filename, PageFormatter fmtr)
	{
		Buffer buff = super.pinNew(filename, fmtr);
		if (buff == null) return null;
		_allocatedBufMap.put(buff.block(), buff);
		return buff;
	}
	
	@Override
	protected synchronized Buffer findExistingBuffer(Block blk)
	{
		return _allocatedBufMap.get(blk);
	}

	@Override
	protected synchronized Buffer chooseUnpinnedBuffer()
	{
		Buffer b = _availBufPool.poll();
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
      for (Buffer buff : _availBufPool) if (buff.isModifiedBy(txnum)) buff.flush();
      for (Buffer buff : _allocatedBufMap.values()) if (buff.isModifiedBy(txnum)) buff.flush();
	}
}
