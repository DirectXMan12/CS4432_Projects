package simpledb.buffer;

import simpledb.file.*;

/**
 * The publicly-accessible buffer manager.
 * A buffer manager wraps a basic buffer manager, and
 * provides the same methods. The difference is that
 * the methods {@link #pin(Block) pin} and 
 * {@link #pinNew(String, PageFormatter) pinNew}
 * will never return null.
 * If no buffers are currently available, then the
 * calling thread will be placed on a waiting list.
 * The waiting threads are removed from the list when 
 * a buffer becomes available.
 * If a thread has been waiting for a buffer for an
 * excessive amount of time (currently, 10 seconds)
 * then a {@link BufferAbortException} is thrown.
 * @author Edward Sciore
 */
public class BufferMgr {
   protected static final long MAX_TIME = 10000; // 10 seconds
   protected AbstractBasicBufferMgr bufferMgr;
   
   /**
    * Creates a new buffer manager having the specified 
    * number of buffers.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffers the number of buffer slots to allocate
    */
   public BufferMgr(int numbuffers)
   {
      bufferMgr = new LRUBasicBufferMgr(numbuffers);
   }
   
   /**
    * Pins a buffer to the specified block, potentially
    * waiting until a buffer becomes available.
    * If no buffer becomes available within a fixed 
    * time period, then a {@link BufferAbortException} is thrown.
    * @param blk a reference to a disk block
    * @return the buffer pinned to that block
    */
   public synchronized AbstractBuffer pin(Block blk) {
      try {
         long timestamp = System.currentTimeMillis();
         AbstractBuffer buff = bufferMgr.pin(blk);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = bufferMgr.pin(blk);
         }
         if (buff == null)
            throw new BufferAbortException();
         return buff;
      }
      catch(InterruptedException e) {
         throw new BufferAbortException();
      }
   }
   
   /**
    * Pins a buffer to a new block in the specified file, 
    * potentially waiting until a buffer becomes available.
    * If no buffer becomes available within a fixed 
    * time period, then a {@link BufferAbortException} is thrown.
    * @param filename the name of the file
    * @param fmtr the formatter used to initialize the page
    * @return the buffer pinned to that block
    */
   public synchronized AbstractBuffer pinNew(String filename, PageFormatter fmtr) {
      try {
         long timestamp = System.currentTimeMillis();
         AbstractBuffer buff = bufferMgr.pinNew(filename, fmtr);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = bufferMgr.pinNew(filename, fmtr);
         }
         if (buff == null)
            throw new BufferAbortException();
         return buff;
      }
      catch(InterruptedException e) {
         throw new BufferAbortException();
      }
   }
   
   /**
    * Unpins the specified buffer. 
    * If the buffer's pin count becomes 0,
    * then the threads on the wait list are notified.
    * @param buff the buffer to be unpinned
    */
   public synchronized void unpin(AbstractBuffer buff) {
      bufferMgr.unpin(buff);
      if (!buff.isPinned())
         notifyAll();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   public void flushAll(int txnum) {
      bufferMgr.flushAll(txnum);
   }
   
   /**
    * Returns the number of available (ie unpinned) buffers.
    * @return the number of available buffers
    */
   public int available() {
      return bufferMgr.available();
   }
   
   protected boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }
}
