package simpledb.tx.concurrency;

import simpledb.file.Block;
import simpledb.tx.LockCheck;
import simpledb.tx.Transaction;

import java.util.*;

/**
 * TODO: Replace doc for all of this file
 */
public class WoundYoungerLockTable
{

	private Map<Block, LockCheck> locks = new HashMap<Block, LockCheck>();
	private Transaction transaction;

	/**
	 * Grants an SLock on the specified block. If an XLock exists when the
	 * method is called, then the calling thread will be placed on a wait list
	 * until the lock is released. If the thread remains on the wait list for a
	 * certain amount of time (currently 10 seconds), then an exception is
	 * thrown.
	 * 
	 * @param blk
	 *            a reference to the disk block
	 */
	public synchronized void sLock(Block blk, Transaction txn) {
		
		LockCheck lock = getLock(blk);
		lock.sLock(txn);

	}

	/**
	 * Grants an XLock on the specified block. If a lock of any type exists when
	 * the method is called, then the calling thread will be placed on a wait
	 * list until the locks are released. If the thread remains on the wait list
	 * for a certain amount of time (currently 10 seconds), then an exception is
	 * thrown.
	 * 
	 * @param blk
	 *            a reference to the disk block
	 */
	public synchronized void xLock(Block blk) {
		LockCheck lock = getLock(blk);
		lock.sLock(transaction);
	}

	/**
	 * Releases a lock on the specified block. If this lock is the last lock on
	 * that block, then the waiting transactions are notified.
	 * 
	 * @param blk
	 *            a reference to the disk block
	 */
	public synchronized void unlock(Block blk) {
		LockCheck lock = getLock(blk);
		lock.sLock(transaction);
	}

	private LockCheck getLock(Block blk) {
		LockCheck lock;
		if (locks.containsKey(blk)) {
			lock = locks.get(blk);
		} else {
			lock = new LockCheck();
		}
		return lock;
	}
}
