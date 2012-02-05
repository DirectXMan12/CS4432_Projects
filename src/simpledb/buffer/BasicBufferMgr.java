package simpledb.buffer;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
public class BasicBufferMgr extends AbstractBasicBufferMgr
{
   protected Buffer[] bufferpool;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   public BasicBufferMgr(int numbuffs)
   {
	  super(numbuffs);
      bufferpool = new Buffer[numbuffs];
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum)
   {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   protected Buffer findExistingBuffer(Block blk) {
      for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;
   }
   
   protected Buffer chooseUnpinnedBuffer() {
      for (Buffer buff : bufferpool)
         if (!buff.isPinned())
         return buff;
      return null;
   }
   
   @Override
   public String toString()
   {
	   return "{alg: \"basic\", totalpool: "+bufferpool+"}";
   }
}
