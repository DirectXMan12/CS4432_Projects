/**
 * 
 */
package simpledb.buffer;

import java.util.Date;

import simpledb.file.Block;

/**
 * @author directxman12
 * Basically, this is just a {@link simpledb.buffer.Buffer Buffer} which
 * keeps track of when it was last used 
 */
public class TimedBuffer extends Buffer implements Comparable<TimedBuffer>
{
	protected long _lastUsed; 

	/**
	 * 
	 */
	public TimedBuffer()
	{
		super();
		_lastUsed = 0;
	}
	
	protected void used()
	{
		_lastUsed = new Date().getTime();
	}
	
	public long getLastUsed()
	{
		return _lastUsed;
	}

	@Override
	public int getInt(int offset)
	{
		used();
		return super.getInt(offset);
	}

	@Override
	public String getString(int offset)
	{
		used();
		return super.getString(offset);
	}

	@Override
	public void setInt(int offset, int val, int txnum, int lsn)
	{
		used();
		super.setInt(offset, val, txnum, lsn);
	}

	@Override
	public void setString(int offset, String val, int txnum, int lsn)
	{
		used();
		super.setString(offset, val, txnum, lsn);
	}

	@Override
	void assignToBlock(Block b)
	{
		used();
		super.assignToBlock(b);
	}

	@Override
	void assignToNew(String filename, PageFormatter fmtr)
	{
		used();
		super.assignToNew(filename, fmtr);
	}

	@Override
	public int compareTo(TimedBuffer o)
	{
		return Long.compare(this.getLastUsed(), o.getLastUsed());
	}
}
