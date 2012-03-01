package simpledb.tx.concurrency;

import simpledb.file.Block;
import simpledb.tx.Transaction;

import java.util.*;

/**
 * TODO: Replace docs for this whole file
 */
public class WoundYoungerConcurrencyMgr
{
   
   /**
    * The global lock table.  This variable is static because all transactions
    * share the same table.
    */
   private static WoundYoungerLockTable locktbl = new WoundYoungerLockTable();
   private Map<Block,String> locks  = new HashMap<Block,String>();
   private Transaction transaction;
   
   /**
    * Obtains an SLock on the block, if necessary.
    * The method will ask the lock table for an SLock
    * if the transaction currently has no locks on that block.
    * @param block a reference to the disk block
    */
   public void sLock(Block block) {
      if (locks.get(block) == null) {
         locktbl.sLock(block, transaction);
         locks.put(block, "S");
      }
   }
   
   /**
    * Obtains an XLock on the block, if necessary.
    * If the transaction does not have an XLock on that block,
    * then the method first gets an SLock on that block
    * (if necessary), and then upgrades it to an XLock.
    * @param block a refrence to the disk block
    */
   public void xLock(Block block) {
      if (!hasXLock(block)) {
         sLock(block);
         locktbl.xLock(block);
         locks.put(block, "X");
      }
   }
   
   /**
    * Releases all locks by asking the lock table to
    * unlock each one.
    */
   public void release() {
      for (Block block : locks.keySet())
         locktbl.unlock(block);
      locks.clear();
   }
   
   private boolean hasXLock(Block block) {
      String locktype = locks.get(block);
      return locktype != null && locktype.equals("X");
   }
}
