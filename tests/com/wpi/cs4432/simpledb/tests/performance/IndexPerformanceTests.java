/**
 * 
 */
package com.wpi.cs4432.simpledb.tests.performance;

import java.sql.SQLException;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.wpi.cs4432.simpledb.tests.SimpleDBBaseTest;

/**
 * @author directxman12
 *
 */
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-lists")
public class IndexPerformanceTests extends SimpleDBBaseTest
{
	public static int randVal;
	
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
	
	@BeforeClass
	public static void initRandConst()
	{
		Random r = new Random();
		randVal = r.nextInt(1000);
	}
	
	@Test
	public void testSimpleHashSelectPerf() throws SQLException
	{
		for (int i = 0; i < 50; i++) stmt.executeQuery("select a11, a12 from test1 where a12 = "+randVal+";");
	}
	
	@Test
	public void testNoIndexSelectPerf() throws SQLException
	{
		for (int i = 0; i < 50; i++) stmt.executeQuery("select a41, a42 from test4 where a42 = "+randVal+";");
	}
	
	@Test
	public void testBPlusTreeSelectPerf() throws SQLException
	{
		for (int i = 0; i < 50; i++) stmt.executeQuery("select a31, a32 from test3 where a32 = "+randVal+";");
	}
	
	@Test
	public void testNoIndexJoinPerf() throws SQLException
	{
		for (int i = 0; i < 50; i++) stmt.executeQuery("select a41, a42, a51, a52 from test4, test5 where a41 = a51;");
	}
	
	@Test
	public void testSimpleHashJoinPerf() throws SQLException
	{
		for (int i = 0; i < 50; i++) stmt.executeQuery("select a11, a12, a51, a52 from test1, test5 where a11 = a51;");
	}
	
	@Test
	public void testBPlusTreeJoinPerf() throws SQLException
	{
		for (int i = 0; i < 50; i++) stmt.executeQuery("select a31, a32, a51, a52 from test3, test5 where a31 = a51;");
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
