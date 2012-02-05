package com.wpi.cs4432.simpledb.tests.functionality;

import static org.junit.Assert.*;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.LinkedList;
import java.util.Random;

import org.apache.derby.iapi.services.io.ArrayUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import simpledb.buffer.BasicBufferMgr;
import simpledb.buffer.BufferMgr;
import simpledb.buffer.LRUBasicBufferMgr;
import simpledb.buffer.MRUBasicBufferMgr;
import simpledb.remote.RemoteDriver;
import simpledb.remote.RemoteDriverImpl;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;
import simpledb.studentClient.CreateStudentDB;

public class BasicSQLFunctionalityTest {
	static Thread serverThread;
	static Driver mainDriver;
	static Connection mainConnection;
	Statement stmt;
	
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		try
		{
			LocateRegistry.createRegistry(1099); // default
		}
		catch(Exception ex)
		{
			
		}
		
		serverThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try
				{
					BufferMgr.setBasicBuffMgrType(LRUBasicBufferMgr.class);
					// configure and initialize the database
					SimpleDB.init("studentdb");
					System.out.println("finished init of SimpleDB");
					// post the server entry in the rmi registry
					RemoteDriver d = new RemoteDriverImpl();
					Naming.rebind("simpledb", d);

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		});
		
		serverThread.start();	
		
		mainDriver = new SimpleDriver();
		String connURL = "jdbc:simpledb://localhost";
		boolean b = true;
		while(b)
		{
			try
			{
				mainConnection = mainDriver.connect(connURL, null);
				
				b = false;
			}
			catch(Exception ex)
			{
				continue;
			}
		}
		
		Statement stmt = mainConnection.createStatement();
		try
		{
			stmt.executeQuery("Select SId from STUDENT");
		}
		catch(SQLException ex)
		{
			CreateStudentDB.main(new String[] {});
		}
	}
	
	
	@Before
	public void setupBefore() throws SQLException
	{
		if (stmt == null) stmt = mainConnection.createStatement();
		stmt.executeUpdate("delete from STUDENT where SId = 10;");
	}
	
	public String[][] getResultSetAsArray(ResultSet rs, String cols[], Class cls[]) throws SQLException
	{
		LinkedList<String[]> res = new LinkedList<String[]>();
		
		while(rs.next())
		{
			String[] row = new String[cols.length];
			for (int i = 0; i < cols.length; i++)
			{
				if(cls[i] == String.class) row[i] = rs.getString(cols[i]);
				else if(cls[i] == Integer.class) row[i] = Integer.toString(rs.getInt(cols[i]));
				else throw new RuntimeException("Ohes noze!");
			}
			res.add(row);
		}
		
		return res.toArray(new String[][] {});
	}
	
	public String stringArrayToString(String[][] strs)
	{
		StringBuilder sb = new StringBuilder("[");
		for (int i = 0; i < strs.length; i++)
		{
			sb.append('[');
			for (int j = 0; j < strs[i].length; j++)
			{
				sb.append(strs[i][j]);
				if (j < strs[i].length - 1) sb.append(",");
			}
			sb.append(']');
			if (i < strs.length - 1) sb.append(',');
		}
		sb.append(']');
		
		return sb.toString();
	}
	
	@Test
	public void testBasicBuffMgrQueries() throws SQLException
	{
		BufferMgr.setBasicBuffMgrType(BasicBufferMgr.class);
		SimpleDB.bufferMgr().resetBasicBufferMgr();
		ResultSet res1 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT;");
		String[][] res1Str = getResultSetAsArray(res1, new String[] { "SId", "SName", "GradYear", "MajorId" }, new Class[] {Integer.class, String.class, Integer.class, Integer.class });
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[3,max,2005,10],[4,sue,2005,20],[5,bob,2003,30],[6,kim,2001,20],[7,art,2004,30],[8,pat,2001,20],[9,lee,2004,10]]", stringArrayToString(res1Str));
		//assertArrayEquals(new String[][] {{"1", "joe", "2004", "10"}, {"2", "amy", "2004", "20"}, {"3", "max", "2005", "10"}, {"4", "sue", "2005", "20"}, {"5", "bob", "2003", "30"}, {"6", "kim", "2001", "20"}, {"7", "art", "2004", "30"}, {"8", "pat", "2001", "20"}, {"9", "lee", "2004", "10"}}, res1Str);
		
		ResultSet res2 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT where gradyear = 2004;");
		String[][] res2Str = getResultSetAsArray(res2, new String[] { "SId", "SName", "GradYear", "MajorId" }, new Class[] {Integer.class, String.class, Integer.class, Integer.class });
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[7,art,2004,30],[9,lee,2004,10]]", stringArrayToString(res2Str));
		//assertArrayEquals(new String[][] {{"1", "joe", "2004", "10"}, {"2", "amy", "2004", "20"}, {"3", "max", "2005", "10"}, {"4", "sue", "2005", "20"}, {"5", "bob", "2003", "30"}, {"6", "kim", "2001", "20"}, {"7", "art", "2004", "30"}, {"8", "pat", "2001", "20"}, {"9", "lee", "2004", "10"}}, res1Str);
		
		ResultSet res3 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT order by MajorId DESC;");
		String[][] res3Str = getResultSetAsArray(res3, new String[] { "SId", "SName", "GradYear", "MajorId" }, new Class[] {Integer.class, String.class, Integer.class, Integer.class });
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[3,max,2005,10],[4,sue,2005,20],[5,bob,2003,30],[6,kim,2001,20],[7,art,2004,30],[8,pat,2001,20],[9,lee,2004,10]]", stringArrayToString(res3Str));
		//assertArrayEquals(new String[][] {{"1", "joe", "2004", "10"}, {"2", "amy", "2004", "20"}, {"3", "max", "2005", "10"}, {"4", "sue", "2005", "20"}, {"5", "bob", "2003", "30"}, {"6", "kim", "2001", "20"}, {"7", "art", "2004", "30"}, {"8", "pat", "2001", "20"}, {"9", "lee", "2004", "10"}}, res1Str);
		
		stmt.executeUpdate("insert into STUDENT(SId, SName, GradYear, MajorId) values (10, 'cheese', 2021, 10);");
		ResultSet res4 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT;");
		String[][] res4Str = getResultSetAsArray(res4, new String[] { "SId", "SName", "GradYear", "MajorId" }, new Class[] {Integer.class, String.class, Integer.class, Integer.class });
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[3,max,2005,10],[4,sue,2005,20],[5,bob,2003,30],[6,kim,2001,20],[7,art,2004,30],[8,pat,2001,20],[9,lee,2004,10],[10,cheese,2021,10]]", stringArrayToString(res4Str));
	}
	
	@Test
	public void testLRUBasicBuffMgrQueries() throws SQLException
	{
		BufferMgr.setBasicBuffMgrType(LRUBasicBufferMgr.class);
		SimpleDB.bufferMgr().resetBasicBufferMgr();
		ResultSet res1 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT;");
		String[][] res1Str = getResultSetAsArray(res1, new String[] { "SId", "SName", "GradYear", "MajorId" }, new Class[] {Integer.class, String.class, Integer.class, Integer.class });
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[3,max,2005,10],[4,sue,2005,20],[5,bob,2003,30],[6,kim,2001,20],[7,art,2004,30],[8,pat,2001,20],[9,lee,2004,10]]", stringArrayToString(res1Str));
		//assertArrayEquals(new String[][] {{"1", "joe", "2004", "10"}, {"2", "amy", "2004", "20"}, {"3", "max", "2005", "10"}, {"4", "sue", "2005", "20"}, {"5", "bob", "2003", "30"}, {"6", "kim", "2001", "20"}, {"7", "art", "2004", "30"}, {"8", "pat", "2001", "20"}, {"9", "lee", "2004", "10"}}, res1Str);
		
		ResultSet res2 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT where gradyear = 2004;");
		String[][] res2Str = getResultSetAsArray(res2, new String[] { "SId", "SName", "GradYear", "MajorId" }, new Class[] {Integer.class, String.class, Integer.class, Integer.class });
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[7,art,2004,30],[9,lee,2004,10]]", stringArrayToString(res2Str));
		//assertArrayEquals(new String[][] {{"1", "joe", "2004", "10"}, {"2", "amy", "2004", "20"}, {"3", "max", "2005", "10"}, {"4", "sue", "2005", "20"}, {"5", "bob", "2003", "30"}, {"6", "kim", "2001", "20"}, {"7", "art", "2004", "30"}, {"8", "pat", "2001", "20"}, {"9", "lee", "2004", "10"}}, res1Str);
		
		ResultSet res3 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT order by MajorId DESC;");
		String[][] res3Str = getResultSetAsArray(res3, new String[] { "SId", "SName", "GradYear", "MajorId" }, new Class[] {Integer.class, String.class, Integer.class, Integer.class });
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[3,max,2005,10],[4,sue,2005,20],[5,bob,2003,30],[6,kim,2001,20],[7,art,2004,30],[8,pat,2001,20],[9,lee,2004,10]]", stringArrayToString(res3Str));
		//assertArrayEquals(new String[][] {{"1", "joe", "2004", "10"}, {"2", "amy", "2004", "20"}, {"3", "max", "2005", "10"}, {"4", "sue", "2005", "20"}, {"5", "bob", "2003", "30"}, {"6", "kim", "2001", "20"}, {"7", "art", "2004", "30"}, {"8", "pat", "2001", "20"}, {"9", "lee", "2004", "10"}}, res1Str);
		
		stmt.executeUpdate("insert into STUDENT(SId, SName, GradYear, MajorId) values (10, 'cheese', 2021, 10);");
		ResultSet res4 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT;");
		String[][] res4Str = getResultSetAsArray(res4, new String[] { "SId", "SName", "GradYear", "MajorId" }, new Class[] {Integer.class, String.class, Integer.class, Integer.class });
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[3,max,2005,10],[4,sue,2005,20],[5,bob,2003,30],[6,kim,2001,20],[7,art,2004,30],[8,pat,2001,20],[9,lee,2004,10],[10,cheese,2021,10]]", stringArrayToString(res4Str));
	}
	
	@Test
	public void testMRUBasicBuffMgrQueries() throws SQLException
	{
		BufferMgr.setBasicBuffMgrType(MRUBasicBufferMgr.class);
		SimpleDB.bufferMgr().resetBasicBufferMgr();
		ResultSet res1 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT;");
		String[][] res1Str = getResultSetAsArray(res1, new String[] { "SId", "SName", "GradYear", "MajorId" }, new Class[] {Integer.class, String.class, Integer.class, Integer.class });
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[3,max,2005,10],[4,sue,2005,20],[5,bob,2003,30],[6,kim,2001,20],[7,art,2004,30],[8,pat,2001,20],[9,lee,2004,10]]", stringArrayToString(res1Str));
		//assertArrayEquals(new String[][] {{"1", "joe", "2004", "10"}, {"2", "amy", "2004", "20"}, {"3", "max", "2005", "10"}, {"4", "sue", "2005", "20"}, {"5", "bob", "2003", "30"}, {"6", "kim", "2001", "20"}, {"7", "art", "2004", "30"}, {"8", "pat", "2001", "20"}, {"9", "lee", "2004", "10"}}, res1Str);
		
		ResultSet res2 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT where gradyear = 2004;");
		String[][] res2Str = getResultSetAsArray(res2, new String[] { "SId", "SName", "GradYear", "MajorId" }, new Class[] {Integer.class, String.class, Integer.class, Integer.class });
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[7,art,2004,30],[9,lee,2004,10]]", stringArrayToString(res2Str));
		//assertArrayEquals(new String[][] {{"1", "joe", "2004", "10"}, {"2", "amy", "2004", "20"}, {"3", "max", "2005", "10"}, {"4", "sue", "2005", "20"}, {"5", "bob", "2003", "30"}, {"6", "kim", "2001", "20"}, {"7", "art", "2004", "30"}, {"8", "pat", "2001", "20"}, {"9", "lee", "2004", "10"}}, res1Str);
		
		ResultSet res3 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT order by MajorId DESC;");
		String[][] res3Str = getResultSetAsArray(res3, new String[] { "SId", "SName", "GradYear", "MajorId" }, new Class[] {Integer.class, String.class, Integer.class, Integer.class });
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[3,max,2005,10],[4,sue,2005,20],[5,bob,2003,30],[6,kim,2001,20],[7,art,2004,30],[8,pat,2001,20],[9,lee,2004,10]]", stringArrayToString(res3Str));
		//assertArrayEquals(new String[][] {{"1", "joe", "2004", "10"}, {"2", "amy", "2004", "20"}, {"3", "max", "2005", "10"}, {"4", "sue", "2005", "20"}, {"5", "bob", "2003", "30"}, {"6", "kim", "2001", "20"}, {"7", "art", "2004", "30"}, {"8", "pat", "2001", "20"}, {"9", "lee", "2004", "10"}}, res1Str);
		
		stmt.executeUpdate("insert into STUDENT(SId, SName, GradYear, MajorId) values (10, 'cheese', 2021, 10);");
		ResultSet res4 = stmt.executeQuery("SELECT SId, SName, GradYear, MajorId from STUDENT;");
		String[][] res4Str = getResultSetAsArray(res4, new String[] { "SId", "SName", "GradYear", "MajorId"}, new Class[] {Integer.class, String.class, Integer.class, Integer.class});
		assertEquals("[[1,joe,2004,10],[2,amy,2004,20],[3,max,2005,10],[4,sue,2005,20],[5,bob,2003,30],[6,kim,2001,20],[7,art,2004,30],[8,pat,2001,20],[9,lee,2004,10],[10,cheese,2021,10]]", stringArrayToString(res4Str));
	}
	
	@After
	public void tearDownAfter() throws SQLException
	{
		//stmt.executeUpdate("delete from STUDENT where SId = 10;");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
		serverThread.stop();
	}
}
