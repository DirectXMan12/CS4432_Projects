/**
 * 
 */
package com.wpi.cs4432.simpledb.tests.performance;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import simpledb.server.SimpleDB;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.wpi.cs4432.simpledb.tests.SimpleDBBaseTest;

/**
 * @author directxman12
 *
 */
@AxisRange(min = 0, max = 1)
@BenchmarkHistoryChart(filePrefix = "benchmark-lists")
public class IndexPerformanceTests extends SimpleDBBaseTest
{
	@Rule
	public MethodRule benchmark = new BenchmarkRule();
	//public static Boolean testCurrentlyRunning = false;

	/*@Before
	public void lockMutex()
	{
		synchronized (this)
		{
			try
			{
				while(testCurrentlyRunning) wait();
				testCurrentlyRunning = true;
				SimpleDB.bufferMgr().resetBasicBufferMgr(); // to make sure buffering doesn't factor in
				return;
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}*/
	
	@Test
	public void testSimpleHashSelectPerf() throws SQLException
	{
		for (int i = 0; i < 100; i++) stmt.executeQuery("select a1, a2 from test1 where a2 = 437;");
	}
	
	@Test
	public void testNoIndexSelectPerf() throws SQLException
	{
		for (int i = 0; i < 100; i++) stmt.executeQuery("select a1, a2 from test4 where a2 = 437;");
	}
	
	@Test
	public void testBPlusTreeSelectPerf() throws SQLException
	{
		for (int i = 0; i < 100; i++) stmt.executeQuery("select a1, a2 from test3 where a2 = 437;");
	}
	
	/*@After
	public void releaseMutex()
	{
		synchronized(this)
		{
			testCurrentlyRunning = false;
			notify();
		}
	}*/
	
	
}
