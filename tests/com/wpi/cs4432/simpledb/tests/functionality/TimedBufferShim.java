/**
 * 
 */
package com.wpi.cs4432.simpledb.tests.functionality;

import java.util.Date;

import simpledb.buffer.AbstractBuffer;
import simpledb.buffer.PageFormatter;
import simpledb.buffer.TimedBuffer;
import simpledb.file.Block;

/**
 * @author directxman12
 *
 */
public class TimedBufferShim extends TimedBuffer
{
	protected long _lastUsed; 

	/**
	 * 
	 */
	public TimedBufferShim()
	{
		super();
		_lastUsed = 0;
	}
	
	@Override
	public synchronized int getInt(int offset)
	{
		used();
		return 0;
	}

	@Override
	public synchronized String getString(int offset)
	{
		used();
		return "";
	}

	@Override
	public synchronized void setInt(int offset, int val, int txnum, int lsn)
	{
		used();
	}

	@Override
	public synchronized void setString(int offset, String val, int txnum, int lsn)
	{
		used();
	}

	@Override
	public synchronized void assignToBlock(Block b)
	{
		used();
		this.blk = b;
	}

	@Override
	public synchronized void assignToNew(String filename, PageFormatter fmtr)
	{
		used();
		this.blk = new Block(filename, 0);
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
		if (obj instanceof TimedBufferShim) return ((TimedBufferShim)obj).block().equals(this.block());
		else return false;
	}

	@Override
	protected void flush()
	{
		// do nothing in the shim
	}
	
	@Override
	public String toString()
	{
		return "TimedBufferShim: blk={" + blk.fileName() + ", " + blk.number() + "}";
	}

}
