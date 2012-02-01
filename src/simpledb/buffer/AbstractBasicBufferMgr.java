/**
 * 
 */
package simpledb.buffer;

import simpledb.file.Block;

/**
 * @author directxman12
 *
 */
public abstract class AbstractBasicBufferMgr
{
	protected int numAvailable;
	
	public AbstractBasicBufferMgr(int numbuffs)
	{
		numAvailable = numbuffs;
	}
	   
	   /**
	* Flushes the dirty buffers modified by the specified transaction.
	* @param txnum the transaction's id number
	*/
	abstract void flushAll(int txnum);
	   
	/**
	* Pins a buffer to the specified block. 
	* If there is already a buffer assigned to that block
	* then that buffer is used;  
	* otherwise, an unpinned buffer from the pool is chosen.
	* Returns a null value if there are no available buffers.
	* @param blk a reference to a disk block
	* @return the pinned buffer
	*/
	synchronized AbstractBuffer pin(Block blk)
	{
		AbstractBuffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
		    if (buff == null) return null;
		    buff.assignToBlock(blk);
		}
		if (!buff.isPinned()) numAvailable--;
		buff.pin();
		return buff;
	}
	   
	/**
	* Allocates a new block in the specified file, and
	* pins a buffer to it. 
	* Returns null (without allocating the block) if 
	* there are no available buffers.
	* @param filename the name of the file
	* @param fmtr a pageformatter object, used to format the new block
	* @return the pinned buffer
	*/
	synchronized AbstractBuffer pinNew(String filename, PageFormatter fmtr)
	{
		AbstractBuffer buff = chooseUnpinnedBuffer();
	    if (buff == null) return null;
	    buff.assignToNew(filename, fmtr);
	    numAvailable--;
	    buff.pin();
	    return buff;
	}
	   
	/**
	* Unpins the specified buffer.
	* @param buff the buffer to be unpinned
	*/
	protected synchronized void unpin(AbstractBuffer buff)
	{
		buff.unpin();
		if (!buff.isPinned()) numAvailable++;
	}
	   
	/**
	* Returns the number of available (i.e. unpinned) buffers.
	* @return the number of available buffers
	*/
	public int available()
	{
		return numAvailable;
	}
	   
	protected abstract AbstractBuffer findExistingBuffer(Block blk);
	   
	protected abstract AbstractBuffer chooseUnpinnedBuffer();
}
