/**
 * 
 */
package simpledb.buffer;

import java.util.Date;
import java.util.Map;

import simpledb.file.Block;

/**
 * @author directxman12
 * Basically, this is just a {@link simpledb.buffer.Buffer Buffer} which
 * keeps track of when it was last used 
 * @param <E>
 */
public class TimedBuffer extends Buffer implements Comparable<TimedBuffer>
{
	protected long _lastUsed; 
	protected SortedQueue<TimedBuffer> _list;
	protected Map<Block, AbstractBuffer> _map;

	/**
	 * 
	 */
	public TimedBuffer()
	{
		super();
		_lastUsed = 0;
	}
	
	public TimedBuffer(SortedQueue<TimedBuffer> l, Map<Block, AbstractBuffer> m)
	{
		super();
		_lastUsed = 0;
		_list = l;
		_map = m;
	}
	
	public void setAvailSet(SortedQueue<TimedBuffer> sortedQueue)
	{
		_list = sortedQueue;
	}
	
	public void setPinnedMap(Map<Block, AbstractBuffer> pinnedMap)
	{
		_map = pinnedMap;
	}
	
	protected synchronized void used()
	{
		if (_list != null)
		{
			_list.remove(this);
			_lastUsed = new Date().getTime();
			_list.add(this);
		}
		else _lastUsed = new Date().getTime();
	}
	
	public synchronized long getLastUsed()
	{
		return _lastUsed;
	}

	@Override
	public synchronized int getInt(int offset)
	{
		used();
		return super.getInt(offset);
	}

	@Override
	public synchronized String getString(int offset)
	{
		used();
		return super.getString(offset);
	}

	@Override
	public synchronized void setInt(int offset, int val, int txnum, int lsn)
	{
		used();
		super.setInt(offset, val, txnum, lsn);
	}

	@Override
	public synchronized void setString(int offset, String val, int txnum, int lsn)
	{
		used();
		super.setString(offset, val, txnum, lsn);
	}

	@Override
	public synchronized void assignToBlock(Block b)
	{
		used();
		super.assignToBlock(b);
	}

	@Override
	public synchronized void assignToNew(String filename, PageFormatter fmtr)
	{
		used();
		super.assignToNew(filename, fmtr);
	}

	@Override
	public int compareTo(TimedBuffer o)
	{
		int res = Long.compare(this.getLastUsed(), o.getLastUsed());
		if (res == 0 && !this.equals(o)) return -1;
		else return res;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null) return false;
		if (this.block() == null && ((TimedBuffer)obj).block() == null) return true;
		else if (this.block() == null || ((TimedBuffer)obj).block() == null) return false;
		if (obj instanceof TimedBuffer) return ((TimedBuffer)obj).block().equals(this.block());
		else return false;
	}

	@Override
	void unpin()
	{
		super.unpin();
		if (pins < 1) 
		{
			if (_list == null)
			{
				System.out.println("null list!");
			}
			if (_list != null) _list.add(this);
			if (_map != null) _map.remove(this);
		}
	}
	

}
