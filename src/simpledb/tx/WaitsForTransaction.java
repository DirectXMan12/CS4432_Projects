package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.PageFormatter;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.concurrency.ConcurrencyMgr;
import simpledb.tx.concurrency.WaitsForConcurrencyMgr;
import simpledb.tx.recovery.RecoveryMgr;

/**
 * TODO: doc
 * @author Edward Sciore
 */
public class WaitsForTransaction extends Transaction
{
   private static int nextTxNum = 0;
   private static final int END_OF_FILE = -1;
   private RecoveryMgr    recoveryMgr;
   private WaitsForConcurrencyMgr concurMgr;
   private int txnum;
   private BufferList myBuffers = new BufferList();
   
   /**
    * Creates a new transaction and its associated 
    * recovery and concurrency managers.
    * This constructor depends on the file, log, and buffer
    * managers that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until either
    * {@link simpledb.server.SimpleDB#init(String)} or 
    * {@link simpledb.server.SimpleDB#initFileLogAndBufferMgr(String)} or
    * is called first.
    */
   public WaitsForTransaction() {
      txnum       = nextTxNumber();
      recoveryMgr = new RecoveryMgr(txnum);
      concurMgr   = new WaitsForConcurrencyMgr();
   }
   
   /**
    * Commits the current transaction.
    * Flushes all modified buffers (and their log records),
    * writes and flushes a commit record to the log,
    * releases all locks, and unpins any pinned buffers.
    */
   @Override
   public void commit() {
      recoveryMgr.commit();
      concurMgr.release(this);
      myBuffers.unpinAll();
      System.out.println("transaction " + txnum + " committed");
   }
   
   /**
    * Rolls back the current transaction.
    * Undoes any modified values,
    * flushes those buffers,
    * writes and flushes a rollback record to the log,
    * releases all locks, and unpins any pinned buffers.
    */
   @Override
   public void rollback() {
      recoveryMgr.rollback();
      concurMgr.release(this);
      myBuffers.unpinAll();
      System.out.println("transaction " + txnum + " rolled back");
   }
   
   /**
    * Returns the integer value stored at the
    * specified offset of the specified block.
    * The method first obtains an SLock on the block,
    * then it calls the buffer to retrieve the value.
    * @param blk a reference to a disk block
    * @param offset the byte offset within the block
    * @return the integer stored at that offset
    */
   @Override
   public int getInt(Block blk, int offset) {
      concurMgr.sLock(blk, this);
      Buffer buff = myBuffers.getBuffer(blk);
      return buff.getInt(offset);
   }
   
   /**
    * Returns the string value stored at the
    * specified offset of the specified block.
    * The method first obtains an SLock on the block,
    * then it calls the buffer to retrieve the value.
    * @param blk a reference to a disk block
    * @param offset the byte offset within the block
    * @return the string stored at that offset
    */
   @Override
   public String getString(Block blk, int offset) {
      concurMgr.sLock(blk, this);
      Buffer buff = myBuffers.getBuffer(blk);
      return buff.getString(offset);
   }
   
   /**
    * Stores an integer at the specified offset 
    * of the specified block.
    * The method first obtains an XLock on the block.
    * It then reads the current value at that offset,
    * puts it into an update log record, and 
    * writes that record to the log.
    * Finally, it calls the buffer to store the value,
    * passing in the LSN of the log record and the transaction's id. 
    * @param blk a reference to the disk block
    * @param offset a byte offset within that block
    * @param val the value to be stored
    */
   @Override
   public void setInt(Block blk, int offset, int val) {
      concurMgr.xLock(blk, this);
      Buffer buff = myBuffers.getBuffer(blk);
      int lsn = recoveryMgr.setInt(buff, offset, val);
      buff.setInt(offset, val, txnum, lsn);
   }
   
   /**
    * Stores a string at the specified offset 
    * of the specified block.
    * The method first obtains an XLock on the block.
    * It then reads the current value at that offset,
    * puts it into an update log record, and 
    * writes that record to the log.
    * Finally, it calls the buffer to store the value,
    * passing in the LSN of the log record and the transaction's id. 
    * @param blk a reference to the disk block
    * @param offset a byte offset within that block
    * @param val the value to be stored
    */
   @Override
   public void setString(Block blk, int offset, String val) {
      concurMgr.xLock(blk, this);
      Buffer buff = myBuffers.getBuffer(blk);
      int lsn = recoveryMgr.setString(buff, offset, val);
      buff.setString(offset, val, txnum, lsn);
   }
   
   /**
    * Returns the number of blocks in the specified file.
    * This method first obtains an SLock on the 
    * "end of the file", before asking the file manager
    * to return the file size.
    * @param filename the name of the file
    * @return the number of blocks in the file
    */
   @Override
   public int size(String filename) {
      Block dummyblk = new Block(filename, END_OF_FILE);
      concurMgr.sLock(dummyblk, this);
      return SimpleDB.fileMgr().size(filename);
   }
   
   /**
    * Appends a new block to the end of the specified file
    * and returns a reference to it.
    * This method first obtains an XLock on the
    * "end of the file", before performing the append.
    * @param filename the name of the file
    * @param fmtr the formatter used to initialize the new page
    * @return a reference to the newly-created disk block
    */
   @Override
   public Block append(String filename, PageFormatter fmtr) {
      Block dummyblk = new Block(filename, END_OF_FILE);
      concurMgr.xLock(dummyblk, this);
      Block blk = myBuffers.pinNew(filename, fmtr);
      unpin(blk);
      return blk;
   }
}
