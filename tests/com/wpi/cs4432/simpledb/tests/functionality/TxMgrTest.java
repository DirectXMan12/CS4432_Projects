package com.wpi.cs4432.simpledb.tests.functionality;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import simpledb.file.Block;
import simpledb.tx.Transaction;

import com.wpi.cs4432.simpledb.tests.SimpleDBBaseTest;
public class TxMgrTest extends SimpleDBBaseTest
{
	@Test
	public void RawTxMgrTest()
	{
		Transaction tx = new Transaction();
		
		Block blk = new Block("testfile", 0);
		tx.pin(blk);

		int ival = tx.getInt(blk, 20);
		String sval = tx.getString(blk, 40);

		tx.setInt(blk, 20, ival+1);
		tx.setString(blk, 40, sval+"1");
		
		tx.unpin(blk);
		tx.commit();

		Transaction tx2 = new Transaction();
		tx2.pin(blk);
		
		assertEquals(tx2.getInt(blk, 20), ival+1);
		assertEquals(tx2.getString(blk, 40), sval+"1");
		
		tx2.unpin(blk);
		tx2.commit();
	}
}
