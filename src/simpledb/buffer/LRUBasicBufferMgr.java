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
	protected LinkedBlockingQueue<Buffer> _availBufPool; // preforms differently if linked vs array
	protected HashMap<Block, Buffer> _allocatedBufMap;
	protected int _queueSize;

	/**
	 * @param numbuffs
	 */
	public LRUBasicBufferMgr(int numbuffs)
	{
		super(numbuffs);
		_queueSize = numbuffs;
		numAvailable = numbuffs;
		_availBufPool = new LinkedBlockingQueue<Buffer>(numbuffs);
		_allocatedBufMap = new HashMap<Block, Buffer>(numbuffs);
		for(int i = 0; i < numbuffs; i++) _availBufPool.add(new Buffer());
		
	}
	
	/**
	 * {@link #pin(Block) pin()}
	 */
	@Override
	synchronized Buffer pin(Block blk)
	{
		Buffer buff = findExistingBuffer(blk); // try to find if the block is in an existing buffer
		if (buff == null) // if not ...
		{
			buff = chooseUnpinnedBuffer(); // grab an unpinned buffer
		    if (buff == null) return null; // (if there's no unpinned, die)
		    _allocatedBufMap.remove(buff.block()); // remove the old mapping
		    buff.assignToBlock(blk); // assign the new block to the old buffer
//		    _ioCount++; // increase the I/O count (slow!)
		}
		if (!buff.isPinned()) // if the buffer was from the available pool
		{
			_availBufPool.remove(buff); // remove it from said pool
		}
		buff.pin(); // pin it
		_allocatedBufMap.put(blk, buff); // put the new mapping into the map
		return buff;
	}
	
	
	@Override
	protected synchronized void unpin(Buffer buff)
	{
		super.unpin(buff);
		if(buff.pins < 1) // add to the pool if there's nothing left using it
		{
			_availBufPool.add(buff);
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
		return _allocatedBufMap.get(blk); // yay hashes
	}

	@Override
	protected synchronized Buffer chooseUnpinnedBuffer()
	{
		Buffer b = _availBufPool.poll(); // FIFO queue functionality -- gets the head
		/*if (_availBufPool.contains(b))
		{
			System.out.println("WTF2!?!");
		}*/
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

	@Override
	public String toString()
	{
		return "simpledb.buffer.LRUBasicBufferMgr@[allocatedMap="+_allocatedBufMap.toString()+", availableQueue=" + _availBufPool.toString()+"]";
	}
}
