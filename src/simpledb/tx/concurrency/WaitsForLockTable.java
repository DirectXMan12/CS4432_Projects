/**
 * 
 */
package simpledb.tx.concurrency;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeSet;

import javax.naming.OperationNotSupportedException;

import simpledb.file.Block;
import simpledb.tx.Transaction;

/**
 * @author directxman12
 *
 */
public class WaitsForLockTable
{
	protected HashMap<Transaction, ArrayList<Transaction>> _edges;
	protected int _maxInd = 0;
	protected TreeSet<Transaction> _nodes;
	protected HashMap<Block, Transaction> _locks;
	
	
	public WaitsForLockTable()
	{
		super();
		_edges = new HashMap<Transaction, ArrayList<Transaction>>();
		_locks = new HashMap<Block, Transaction>();
		_nodes = new TreeSet<Transaction>();
	}
	
	public synchronized void sLock(Block blk, Transaction trans)
	{
	}
	
	protected synchronized void addEdge(Transaction src, Transaction dest)
	{
		if (!_edges.containsKey(src))
		{
			ArrayList<Transaction> ar = new ArrayList<Transaction>();
			ar.add(dest);
			_edges.put(src, ar);
		}
		else
		{
			_edges.get(src).add(dest);
		}
	}
	
	public synchronized void xLock(Block blk, Transaction trans)
	{
		_nodes.add(trans);
		if (_locks.get(blk) != null)
		{
			addEdge(trans, _locks.get(blk));
		}
		else
		{
			_locks.put(blk, trans);
		}
		if (hasCycle(blk, trans))
		{
			_edges.get(blk).remove(_locks.get(blk));
			_nodes.remove(trans);
			throw new LockAbortException();
		}
	}
	
	public synchronized void unlock(Block blk)
	{
		// unlock now
	}
}