package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.*;

/**
 * An individual buffer.
 * A buffer wraps a page and stores information about its status,
 * such as the disk block associated with the page,
 * the number of times the block has been pinned,
 * whether the contents of the page have been modified,
 * and if so, the id of the modifying transaction and
 * the LSN of the corresponding log record.
 * @author Edward Sciore
 */
public class Buffer extends AbstractBuffer {
   private Page contents = new Page();
   private int logSequenceNumber = -1; // negative means no corresponding log record

   /**
    * Creates a new buffer, wrapping a new 
    * {@link simpledb.file.Page page}.  
    * This constructor is called exclusively by the 
    * class {@link AbstractBasicBufferMgr}.   
    * It depends on  the 
    * {@link simpledb.log.LogMgr LogMgr} object 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * That object is created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    */
   public Buffer() {}
   
   /**
    * Returns the integer value at the specified offset of the
    * buffer's page.
    * If an integer was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the integer value at that offset
    */
   public int getInt(int offset) {
      return contents.getInt(offset);
   }

   /**
    * Returns the string value at the specified offset of the
    * buffer's page.
    * If a string was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the string value at that offset
    */
   public String getString(int offset) {
      return contents.getString(offset);
   }

   /**
    * Writes an integer to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * @param offset the byte offset within the page
    * @param val the new integer value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    */
   public void setInt(int offset, int val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setInt(offset, val);
   }

   /**
    * Writes a string to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * @param offset the byte offset within the page
    * @param val the new string value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    */
   public void setString(int offset, String val, int txnum, int lsn) {
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setString(offset, val);
   }

   /**
    * Writes the page to its disk block if the
    * page is dirty.
    * The method ensures that the corresponding log
    * record has been written to disk prior to writing
    * the page to disk.
    */
   protected void flush() {
      if (modifiedBy >= 0) {
         SimpleDB.logMgr().flush(logSequenceNumber);
         contents.write(blk);
         modifiedBy = -1;
      }
   }

   /**
    * Reads the contents of the specified block into
    * the buffer's page.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param b a reference to the data block
    */
   protected void assignToBlock(Block b) {
      flush();
      blk = b;
      contents.read(blk);
      pins = 0;
   }

   /**
    * Initializes the buffer's page according to the specified formatter,
    * and appends the page to the specified file.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param filename the name of the file
    * @param fmtr a page formatter, used to initialize the page
    */
   protected void assignToNew(String filename, PageFormatter fmtr) {
      flush();
      fmtr.format(contents);
      blk = contents.append(filename);
      pins = 0;
   }
}