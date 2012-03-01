package com.wpi.cs4432.simpledb.tests.functionality;

import static org.junit.Assert.assertEquals;

import java.rmi.Naming;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import simpledb.file.Block;
import simpledb.remote.RemoteDriver;
import simpledb.remote.RemoteDriverImpl;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.tx.WaitsForTransaction;
import simpledb.tx.WoundYoungerTransaction;

import com.wpi.cs4432.simpledb.tests.CreateTestTables;
import com.wpi.cs4432.simpledb.tests.SimpleDBBaseTest;
public class TxMgrTest extends SimpleDBBaseTest
{
	@Test
	public void RawTxMgrTest()
	{
		
		Transaction tx = new WaitsForTransaction();
		//Transaction tx = new WaitsForTransaction();
		//Transaction tx = new Transaction();
		
		Block blk = new Block("testfile", 0);
		tx.pin(blk); // T1 lock

		int ival = tx.getInt(blk, 20);
		String sval = tx.getString(blk, 40);

		// should this throw an error?
		tx.setInt(blk, 20, ival+1);
		tx.setString(blk, 40, sval+"1");
		
		tx.unpin(blk); // T1 Unlock
		tx.commit();

		Transaction tx2 = new WaitsForTransaction();
		//Transaction tx2 = new WaitsForTransaction();
		//Transaction tx2 = new Transaction();
		tx2.pin(blk); // T2 Lock
		
		assertEquals(tx2.getInt(blk, 20), ival+1);
		assertEquals(tx2.getString(blk, 40), sval+"1");
		
		tx2.unpin(blk); // T2 unlock
		tx2.commit();
	}
	
	@Test
	public void BlackTxMgrTest() throws Exception
	{
		
		try
		{
				SimpleDBBaseTest.setUpBeforeClass();
				ResultSet rs = stmt.executeQuery("select a51 from test5;");
				System.out.println(rs.toString());
				
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
	}
}
