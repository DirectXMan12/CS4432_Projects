/**
 * 
 */
package simpledb.buffer;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingDeque;

import simpledb.file.Block;

/**
 * Basic Buffer Manager implementation that uses an MRU (Most Recently Used) algorithm to manage
 * buffer usage.
 * @author jeffnamias
 */
public class MRUBasicBufferMgr extends AbstractBasicBufferMgr
{
	protected LinkedBlockingDeque<Buffer> _availBufPool; // preforms differently if linked vs array
	protected HashMap<Block, Buffer> _allocatedBufMap;
	protected int _queueSize;

	/**
	 * Creates an instance of a MRUBasicBufferMgr with a specified number of buffers. 
	 * @param numbuffs the number of buffers
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
	
	/**
    * {@inheritDoc}
    */
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
	
	/**
    * {@inheritDoc}
    */
	@Override
	protected synchronized void unpin(Buffer buff)
	{
		super.unpin(buff);
		if(buff.pins < 1 /* && !_availBufPool.contains(buff) */)
		{
			_availBufPool.addFirst(buff); // adds to the head, not the tail like LRU
		}
	}

	/**
    * {@inheritDoc}
    */
	@Override
	synchronized Buffer pinNew(String filename, PageFormatter fmtr)
	{
		Buffer buff = super.pinNew(filename, fmtr);
		if (buff == null) return null;
		_allocatedBufMap.put(buff.block(), buff);
		return buff;
	}

	/**
    * {@inheritDoc}
    */
	@Override
	protected synchronized Buffer findExistingBuffer(Block blk)
	{
		return _allocatedBufMap.get(blk);
	}
	
	/**
    * {@inheritDoc}
    */
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

	/**
    * {@inheritDoc}
    */
	@Override
	public int available()
	{
		return _availBufPool.size();
	}

	/**
    * {@inheritDoc}
    */
	@Override
	void flushAll(int txnum)
	{
      for (Buffer buff : _availBufPool) if (buff.isModifiedBy(txnum)) buff.flush();
      for (Buffer buff : _allocatedBufMap.values()) if (buff.isModifiedBy(txnum)) buff.flush();
	}

	/**
    * {@inheritDoc}
    */
	@Override
	public String toString()
	{
		return "{alg: \"MRU\", allocated: "+_allocatedBufMap.toString()+", available: " + _availBufPool.toString()+"}";
	}
}
